package com.orderflow.order;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.events.*;
import com.orderflow.order.domain.Order;
import com.orderflow.order.domain.OrderStatus;
import com.orderflow.order.dto.OrderDtos;
import com.orderflow.order.repo.OrderRepository;
import com.orderflow.order.saga.OrderService;
import com.orderflow.order.support.AbstractIntegrationTest;
import com.orderflow.order.support.TestBus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The saga, driven end to end through Kafka.
 *
 * <p>Nothing here calls the orchestrator directly. Orders go in through the
 * service, replies come back over the bus, and assertions are made on the
 * database and on the messages that were actually published — so these tests
 * cover the outbox relay, JSON serialisation, message headers and consumer
 * wiring in addition to the state machine itself.
 */
@AbstractIntegrationTest.IntegrationTest
@DisplayName("Order saga over Kafka")
class SagaFlowIT extends AbstractIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired OrderService orderService;
    @Autowired OrderRepository orders;
    @Autowired ObjectMapper json;

    private TestBus bus;

    @BeforeEach
    void setUp() {
        bus = new TestBus(bootstrapServers(), json);
    }

    @AfterEach
    void tearDown() {
        bus.close();
    }

    @Test
    @DisplayName("Happy path: reserved, then authorised, ends CONFIRMED")
    void happyPath() {
        var inventoryCommands = bus.listenTo(Topics.INVENTORY_COMMANDS);
        var paymentCommands = bus.listenTo(Topics.PAYMENT_COMMANDS);

        OrderDtos.OrderResponse created = placeOrder("cust-happy", "WIDGET-BLUE", 2, 5_00L);
        UUID sagaId = created.sagaId();

        // Step 1: the outbox relay published the first command.
        InventoryCommand.Reserve reserve = inventoryCommands.awaitMessage(
                "ReserveInventory", InventoryCommand.Reserve.class, TIMEOUT);
        assertThat(reserve.sagaId()).isEqualTo(sagaId);
        assertThat(reserve.lines()).hasSize(1);

        // Step 2: inventory replies successfully.
        bus.publish(Topics.INVENTORY_EVENTS, sagaId, "InventoryReserved",
                new InventoryEvent.Reserved(sagaId, created.id(), "RES-TEST-1", Instant.now()));

        PaymentCommand.Authorize authorize = paymentCommands.awaitMessage(
                "AuthorizePayment", PaymentCommand.Authorize.class, TIMEOUT);
        assertThat(authorize.amountCents())
                .as("the saga must charge the order total, not a line price")
                .isEqualTo(10_00L);
        assertThat(authorize.idempotencyKey())
                .as("a redelivered command must carry a stable key or the customer is charged twice")
                .contains(sagaId.toString());

        awaitStatus(created.id(), OrderStatus.INVENTORY_RESERVED);

        // Step 3: payment succeeds.
        bus.publish(Topics.PAYMENT_EVENTS, sagaId, "PaymentAuthorized",
                new PaymentEvent.Authorized(sagaId, created.id(), "TXN-TEST-1", 10_00L, Instant.now()));

        awaitStatus(created.id(), OrderStatus.CONFIRMED);

        Order finished = orders.findWithLinesById(created.id()).orElseThrow();
        assertThat(finished.getPaymentRef()).isEqualTo("TXN-TEST-1");
        assertThat(finished.getReservationRef()).isEqualTo("RES-TEST-1");
        assertThat(finished.getFailureReason()).isNull();
    }

    @Test
    @DisplayName("Payment declined after stock was reserved: the saga compensates")
    void compensatesWhenPaymentFailsAfterReservation() {
        var inventoryCommands = bus.listenTo(Topics.INVENTORY_COMMANDS);

        OrderDtos.OrderResponse created = placeOrder("cust-decline", "GIZMO-LARGE", 1, 250_00L);
        UUID sagaId = created.sagaId();

        inventoryCommands.awaitMessage("ReserveInventory", InventoryCommand.Reserve.class, TIMEOUT);

        bus.publish(Topics.INVENTORY_EVENTS, sagaId, "InventoryReserved",
                new InventoryEvent.Reserved(sagaId, created.id(), "RES-TEST-2", Instant.now()));
        awaitStatus(created.id(), OrderStatus.INVENTORY_RESERVED);

        // The failure that matters: stock is already held in another service.
        bus.publish(Topics.PAYMENT_EVENTS, sagaId, "PaymentFailed",
                new PaymentEvent.Failed(sagaId, created.id(), "INSUFFICIENT_FUNDS",
                        "Card declined", Instant.now()));

        // Cancelling without releasing would strand that stock permanently, so
        // the saga must emit a compensating command rather than just give up.
        InventoryCommand.Release release = inventoryCommands.awaitMessage(
                "ReleaseInventory", InventoryCommand.Release.class, TIMEOUT);
        assertThat(release.sagaId()).isEqualTo(sagaId);
        assertThat(release.reason()).contains("INSUFFICIENT_FUNDS");

        // And it stays COMPENSATING, not CANCELLED, until the release is
        // acknowledged. Marking it cancelled early would hide the fact that
        // inventory is still held.
        awaitStatus(created.id(), OrderStatus.COMPENSATING);

        bus.publish(Topics.INVENTORY_EVENTS, sagaId, "InventoryReleased",
                new InventoryEvent.Released(sagaId, created.id(), Instant.now()));

        awaitStatus(created.id(), OrderStatus.CANCELLED);
        assertThat(orders.findWithLinesById(created.id()).orElseThrow().getFailureReason())
                .contains("INSUFFICIENT_FUNDS");
    }

    @Test
    @DisplayName("Out of stock: cancels without compensating, because nothing was taken")
    void cancelsWithoutCompensationWhenStockIsUnavailable() {
        var inventoryCommands = bus.listenTo(Topics.INVENTORY_COMMANDS);
        var paymentCommands = bus.listenTo(Topics.PAYMENT_COMMANDS);

        OrderDtos.OrderResponse created = placeOrder("cust-oos", "LOW-STOCK-1", 99, 1_00L);
        UUID sagaId = created.sagaId();

        inventoryCommands.awaitMessage("ReserveInventory", InventoryCommand.Reserve.class, TIMEOUT);

        bus.publish(Topics.INVENTORY_EVENTS, sagaId, "InventoryReservationFailed",
                new InventoryEvent.ReservationFailed(sagaId, created.id(),
                        "Insufficient stock for LOW-STOCK-1", "LOW-STOCK-1", Instant.now()));

        awaitStatus(created.id(), OrderStatus.CANCELLED);

        // Nothing was reserved and nothing was charged, so there is nothing to
        // undo. Emitting a release here would be a bug, not extra safety: the
        // inventory service would decrement a reservation that never existed.
        paymentCommands.drainOnce();
        assertThat(paymentCommands.seenTypes())
                .as("payment must never be asked to charge an order that failed at step one")
                .doesNotContain("AuthorizePayment");
        assertThat(inventoryCommands.countOf("ReleaseInventory"))
                .as("nothing was reserved, so nothing may be released")
                .isZero();
    }

    @Test
    @DisplayName("A redelivered reply is applied once")
    void duplicateReplyIsIgnored() {
        var inventoryCommands = bus.listenTo(Topics.INVENTORY_COMMANDS);
        var paymentCommands = bus.listenTo(Topics.PAYMENT_COMMANDS);

        OrderDtos.OrderResponse created = placeOrder("cust-dupe", "WIDGET-RED", 1, 20_00L);
        UUID sagaId = created.sagaId();
        inventoryCommands.awaitMessage("ReserveInventory", InventoryCommand.Reserve.class, TIMEOUT);

        // The same message id twice, exactly as an at-least-once broker would
        // redeliver after a consumer crashed between handling and committing.
        UUID messageId = UUID.randomUUID();
        var reserved = new InventoryEvent.Reserved(sagaId, created.id(), "RES-DUPE", Instant.now());
        bus.publishWithMessageId(Topics.INVENTORY_EVENTS, sagaId, messageId, "InventoryReserved", reserved);
        bus.publishWithMessageId(Topics.INVENTORY_EVENTS, sagaId, messageId, "InventoryReserved", reserved);

        awaitStatus(created.id(), OrderStatus.INVENTORY_RESERVED);
        paymentCommands.awaitMessage("AuthorizePayment", PaymentCommand.Authorize.class, TIMEOUT);

        // Give the second delivery time to be wrongly processed, then confirm it
        // was not: one authorisation request, not two.
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            paymentCommands.drainOnce();
            assertThat(paymentCommands.countOf("AuthorizePayment"))
                    .as("a duplicate reply must not trigger a second charge")
                    .isEqualTo(1);
        });
    }

    // ------------------------------------------------------------------ helpers

    private OrderDtos.OrderResponse placeOrder(String customer, String sku, int qty, long unitPrice) {
        return orderService.create(new OrderDtos.CreateOrderRequest(
                customer, List.of(new OrderDtos.LineRequest(sku, qty, unitPrice))));
    }

    private void awaitStatus(Long orderId, OrderStatus expected) {
        await().atMost(TIMEOUT).pollInterval(Duration.ofMillis(200)).untilAsserted(() ->
                assertThat(orders.findById(orderId).orElseThrow().getStatus())
                        .as("order %s should reach %s", orderId, expected)
                        .isEqualTo(expected));
    }
}
