package com.orderflow.order.saga;

import com.orderflow.events.*;
import com.orderflow.order.domain.Order;
import com.orderflow.order.domain.OrderStatus;
import com.orderflow.messaging.OutboxWriter;
import com.orderflow.order.repo.OrderRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * The saga orchestrator: the one place that knows what an order does next.
 *
 * <h2>Why a saga at all</h2>
 *
 * <p>Confirming an order means reserving stock in one service and taking money
 * in another. Those live in different databases, so there is no transaction that
 * can span them. Two-phase commit could in principle, but it holds locks across
 * a network for the duration and makes every participant's availability depend
 * on every other's — a coordinator that dies mid-decision leaves resources
 * locked until a human intervenes.
 *
 * <p>A saga accepts the trade instead: each step commits locally and
 * immediately, and if a later step fails, earlier steps are undone by explicit
 * <em>compensating</em> actions. The system is never globally locked, but it is
 * only <em>eventually</em> consistent, and there is a real window where stock is
 * reserved for an order that will never be paid for.
 *
 * <h2>Orchestration rather than choreography</h2>
 *
 * <p>The alternative is choreography: each service listens for the previous
 * one's event and decides for itself what to do. That needs no coordinator, but
 * the flow exists only as an emergent property of who happens to subscribe to
 * what, and answering "why did order 41 stall?" means reading every service.
 * Here the flow is one readable state machine, and its cost is that this class
 * knows the names of the steps.
 *
 * <pre>
 *   PENDING ──Reserved──▶ INVENTORY_RESERVED ──Authorized──▶ CONFIRMED
 *      │                          │
 *      │ReservationFailed         │PaymentFailed  ── emits ReleaseInventory
 *      ▼                          ▼
 *   CANCELLED ◀──Released── COMPENSATING
 * </pre>
 *
 * <h2>Every handler is idempotent</h2>
 *
 * <p>Message ids are deduplicated one layer up, but that is not relied upon
 * alone. Each handler re-reads the order and checks the status it expects, so a
 * duplicate or late-arriving reply is ignored rather than driving a second
 * transition. Defence in depth matters here because the failure mode is silent:
 * a double-applied transition produces a plausible-looking order in the wrong
 * state, discovered days later.
 */
@Service
public class OrderSaga {

    private static final Logger log = LoggerFactory.getLogger(OrderSaga.class);
    private static final String AGGREGATE = "Order";

    private final OrderRepository orders;
    private final OutboxWriter outbox;
    private final Counter confirmed;
    private final Counter cancelled;
    private final Counter compensations;

    public OrderSaga(OrderRepository orders, OutboxWriter outbox, MeterRegistry metrics) {
        this.orders = orders;
        this.outbox = outbox;
        this.confirmed = Counter.builder("orderflow.saga").tag("outcome", "confirmed").register(metrics);
        this.cancelled = Counter.builder("orderflow.saga").tag("outcome", "cancelled").register(metrics);
        // Worth watching on its own: a rising compensation rate means money and
        // stock are being moved and then moved back, which costs real money in
        // payment-provider fees even when every order ends in a correct state.
        this.compensations = Counter.builder("orderflow.saga.compensations").register(metrics);
    }

    // ------------------------------------------------------------- step 1 reply

    @Transactional
    public void handle(InventoryEvent event) {
        if (event instanceof InventoryEvent.Reserved reserved) {
            onInventoryReserved(reserved);
        } else if (event instanceof InventoryEvent.ReservationFailed failed) {
            onInventoryReservationFailed(failed);
        } else if (event instanceof InventoryEvent.Released released) {
            onInventoryReleased(released);
        }
    }

    private void onInventoryReserved(InventoryEvent.Reserved event) {
        Optional<Order> found = load(event.sagaId());
        if (found.isEmpty()) {
            return;
        }
        Order order = found.get();

        if (order.getStatus() != OrderStatus.PENDING) {
            // Either a duplicate, or a reply that lost a race against a
            // cancellation. Neither is an error; both must be ignored.
            log.debug("Ignoring InventoryReserved for order {} in status {}", order.getId(), order.getStatus());
            return;
        }

        order.setStatus(OrderStatus.INVENTORY_RESERVED);
        order.setReservationRef(event.reservationRef());

        // Stock now exists in another service on this order's behalf. From here
        // on, any failure obliges us to give it back.
        outbox.enqueue(order.getSagaId(), AGGREGATE, String.valueOf(order.getId()),
                Topics.PAYMENT_COMMANDS, "AuthorizePayment",
                new PaymentCommand.Authorize(
                        order.getSagaId(),
                        order.getId(),
                        order.getCustomerRef(),
                        order.getTotalCents(),
                        // Derived from the saga id, not random: a redelivered
                        // command must carry the same key, or the provider will
                        // treat the retry as a second purchase.
                        "order-" + order.getId() + "-" + order.getSagaId()));

        log.info("Saga {} order {}: inventory reserved, requesting payment of {} cents",
                order.getSagaId(), order.getId(), order.getTotalCents());
    }

    private void onInventoryReservationFailed(InventoryEvent.ReservationFailed event) {
        Optional<Order> found = load(event.sagaId());
        if (found.isEmpty()) {
            return;
        }
        Order order = found.get();

        if (order.getStatus() != OrderStatus.PENDING) {
            log.debug("Ignoring ReservationFailed for order {} in status {}", order.getId(), order.getStatus());
            return;
        }

        // Nothing was reserved and nothing was charged, so there is nothing to
        // compensate. This is the cheap failure, and the reason inventory is
        // checked before payment rather than after.
        cancel(order, event.reason());
    }

    private void onInventoryReleased(InventoryEvent.Released event) {
        Optional<Order> found = load(event.sagaId());
        if (found.isEmpty()) {
            return;
        }
        Order order = found.get();

        if (order.getStatus() != OrderStatus.COMPENSATING) {
            log.debug("Ignoring InventoryReleased for order {} in status {}", order.getId(), order.getStatus());
            return;
        }

        // The last outstanding side effect is undone; the saga can now finish.
        cancel(order, order.getFailureReason());
    }

    // ------------------------------------------------------------- step 2 reply

    @Transactional
    public void handle(PaymentEvent event) {
        if (event instanceof PaymentEvent.Authorized authorized) {
            onPaymentAuthorized(authorized);
        } else if (event instanceof PaymentEvent.Failed failed) {
            onPaymentFailed(failed);
        }
        // PaymentEvent.Refunded needs no handling: this saga only refunds as
        // part of a flow that has already reached a terminal state.
    }

    private void onPaymentAuthorized(PaymentEvent.Authorized event) {
        Optional<Order> found = load(event.sagaId());
        if (found.isEmpty()) {
            return;
        }
        Order order = found.get();

        if (order.getStatus() != OrderStatus.INVENTORY_RESERVED) {
            log.debug("Ignoring PaymentAuthorized for order {} in status {}", order.getId(), order.getStatus());
            return;
        }

        order.setStatus(OrderStatus.CONFIRMED);
        order.setPaymentRef(event.transactionRef());

        outbox.enqueue(order.getSagaId(), AGGREGATE, String.valueOf(order.getId()),
                Topics.ORDER_EVENTS, "OrderConfirmed",
                new OrderEvent.Confirmed(order.getSagaId(), order.getId(),
                        event.transactionRef(), order.getTotalCents(), Instant.now()));

        confirmed.increment();
        log.info("Saga {} order {}: confirmed against payment {}",
                order.getSagaId(), order.getId(), event.transactionRef());
    }

    /**
     * The interesting failure: stock is already reserved and the card was
     * declined. Cancelling the order alone would silently strand that stock,
     * where it would sit unsellable until someone noticed the discrepancy.
     */
    private void onPaymentFailed(PaymentEvent.Failed event) {
        Optional<Order> found = load(event.sagaId());
        if (found.isEmpty()) {
            return;
        }
        Order order = found.get();

        if (order.getStatus() != OrderStatus.INVENTORY_RESERVED) {
            log.debug("Ignoring PaymentFailed for order {} in status {}", order.getId(), order.getStatus());
            return;
        }

        // Not CANCELLED yet. The order stays COMPENSATING until inventory
        // confirms the release, so a crash right here leaves the true state
        // visible rather than a cancelled order with stock still held against it.
        order.setStatus(OrderStatus.COMPENSATING);
        order.setFailureReason("Payment declined: " + event.declineCode());

        outbox.enqueue(order.getSagaId(), AGGREGATE, String.valueOf(order.getId()),
                Topics.INVENTORY_COMMANDS, "ReleaseInventory",
                new InventoryCommand.Release(order.getSagaId(), order.getId(),
                        "payment declined (" + event.declineCode() + ")"));

        compensations.increment();
        log.warn("Saga {} order {}: payment failed ({}), compensating by releasing inventory",
                order.getSagaId(), order.getId(), event.declineCode());
    }

    // ----------------------------------------------------------------- helpers

    private void cancel(Order order, String reason) {
        order.setStatus(OrderStatus.CANCELLED);
        order.setFailureReason(reason);

        outbox.enqueue(order.getSagaId(), AGGREGATE, String.valueOf(order.getId()),
                Topics.ORDER_EVENTS, "OrderCancelled",
                new OrderEvent.Cancelled(order.getSagaId(), order.getId(), reason, Instant.now()));

        cancelled.increment();
        log.info("Saga {} order {}: cancelled ({})", order.getSagaId(), order.getId(), reason);
    }

    private Optional<Order> load(UUID sagaId) {
        Optional<Order> order = orders.findWithLinesBySagaId(sagaId);
        if (order.isEmpty()) {
            // A reply for a saga this service has no record of. Logged rather
            // than thrown: retrying cannot conjure the order into existence, and
            // failing would send a harmless message to the dead-letter topic.
            log.warn("Received a saga reply for unknown saga {}", sagaId);
        }
        return order;
    }
}
