package com.orderflow.inventory;

import com.orderflow.events.InventoryCommand;
import com.orderflow.events.InventoryEvent;
import com.orderflow.events.OrderLine;
import com.orderflow.events.Topics;
import com.orderflow.messaging.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Reserves and releases stock on behalf of the order saga.
 *
 * <p>Both operations write their reply to the outbox in the same transaction
 * that changes the stock. The alternative — update the database, then publish —
 * has a failure window in which stock is reserved but the order service is never
 * told, leaving the saga stalled forever with inventory quietly held against an
 * order that will never progress.
 */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
    private static final String AGGREGATE = "Reservation";

    private final StockRepository stock;
    private final ReservationRepository reservations;
    private final OutboxWriter outbox;

    public InventoryService(StockRepository stock, ReservationRepository reservations, OutboxWriter outbox) {
        this.stock = stock;
        this.reservations = reservations;
        this.outbox = outbox;
    }

    @Transactional
    public void reserve(InventoryCommand.Reserve command) {
        // A redelivered command must not reserve a second time. The saga id is
        // unique on the reservation table, so this check and that constraint
        // together make the operation safely repeatable.
        Optional<Reservation> existing = reservations.findWithLinesBySagaId(command.sagaId());
        if (existing.isPresent()) {
            Reservation reservation = existing.get();
            log.debug("Reservation for saga {} already exists in status {}; replying again",
                    command.sagaId(), reservation.getStatus());
            // Reply again rather than staying silent: the original reply may be
            // exactly what went missing and caused this redelivery.
            emitReserved(reservation);
            return;
        }

        List<OrderLine> lines = command.lines();
        Map<String, Integer> wanted = lines.stream().collect(Collectors.toMap(
                OrderLine::sku, OrderLine::quantity, Integer::sum));

        Map<String, StockItem> items = stock.findBySkuInOrderBySku(wanted.keySet()).stream()
                .collect(Collectors.toMap(StockItem::getSku, Function.identity()));

        // Check everything before changing anything. A partial reservation is
        // worse than none: it holds stock the order can never use, and the saga
        // would still be told the whole thing failed.
        for (Map.Entry<String, Integer> entry : wanted.entrySet()) {
            StockItem item = items.get(entry.getKey());
            if (item == null) {
                emitFailure(command, "Unknown SKU " + entry.getKey(), entry.getKey());
                return;
            }
            if (!item.canReserve(entry.getValue())) {
                emitFailure(command,
                        "Insufficient stock for " + entry.getKey()
                                + ": wanted " + entry.getValue() + ", available " + item.available(),
                        entry.getKey());
                return;
            }
        }

        Reservation reservation = Reservation.builder()
                .sagaId(command.sagaId())
                .orderId(command.orderId())
                .reservationRef("RES-" + command.orderId() + "-" + command.sagaId().toString().substring(0, 8))
                .status(Reservation.Status.HELD)
                .build();

        wanted.forEach((sku, quantity) -> {
            items.get(sku).reserve(quantity);
            reservation.addLine(sku, quantity);
        });

        reservations.saveAndFlush(reservation);
        emitReserved(reservation);

        log.info("Saga {} order {}: reserved {} SKUs as {}",
                command.sagaId(), command.orderId(), wanted.size(), reservation.getReservationRef());
    }

    /**
     * The compensating action. Puts stock back after a downstream failure.
     */
    @Transactional
    public void release(InventoryCommand.Release command) {
        Optional<Reservation> found = reservations.findWithLinesBySagaId(command.sagaId());
        if (found.isEmpty()) {
            // Nothing was ever held for this saga, so there is nothing to undo.
            // Still reply: a compensation that produces no acknowledgement leaves
            // the saga waiting for a message that is never coming.
            log.warn("Release requested for unknown saga {}; acknowledging anyway", command.sagaId());
            emitReleased(command.sagaId(), command.orderId());
            return;
        }

        Reservation reservation = found.get();
        if (reservation.getStatus() == Reservation.Status.RELEASED) {
            log.debug("Reservation for saga {} was already released; replying again", command.sagaId());
            emitReleased(command.sagaId(), command.orderId());
            return;
        }

        Map<String, StockItem> items = stock.findBySkuInOrderBySku(
                        reservation.getLines().stream().map(ReservationLine::getSku).toList()).stream()
                .collect(Collectors.toMap(StockItem::getSku, Function.identity()));

        reservation.getLines().forEach(line -> {
            StockItem item = items.get(line.getSku());
            if (item != null) {
                item.release(line.getQuantity());
            }
        });

        reservation.setStatus(Reservation.Status.RELEASED);
        reservation.setReleasedAt(Instant.now());
        reservations.save(reservation);

        emitReleased(command.sagaId(), command.orderId());
        log.info("Saga {} order {}: released reservation ({})",
                command.sagaId(), command.orderId(), command.reason());
    }

    private void emitReserved(Reservation reservation) {
        outbox.enqueue(reservation.getSagaId(), AGGREGATE, String.valueOf(reservation.getOrderId()),
                Topics.INVENTORY_EVENTS, "InventoryReserved",
                new InventoryEvent.Reserved(reservation.getSagaId(), reservation.getOrderId(),
                        reservation.getReservationRef(), Instant.now()));
    }

    private void emitFailure(InventoryCommand.Reserve command, String reason, String sku) {
        outbox.enqueue(command.sagaId(), AGGREGATE, String.valueOf(command.orderId()),
                Topics.INVENTORY_EVENTS, "InventoryReservationFailed",
                new InventoryEvent.ReservationFailed(command.sagaId(), command.orderId(),
                        reason, sku, Instant.now()));
        log.info("Saga {} order {}: reservation refused ({})", command.sagaId(), command.orderId(), reason);
    }

    private void emitReleased(java.util.UUID sagaId, Long orderId) {
        outbox.enqueue(sagaId, AGGREGATE, String.valueOf(orderId),
                Topics.INVENTORY_EVENTS, "InventoryReleased",
                new InventoryEvent.Released(sagaId, orderId, Instant.now()));
    }
}
