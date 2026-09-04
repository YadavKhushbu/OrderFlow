package com.orderflow.order;

import com.orderflow.events.Topics;
import com.orderflow.messaging.OutboxWriter;
import com.orderflow.order.dto.OrderDtos;
import com.orderflow.order.repo.OrderRepository;
import com.orderflow.order.saga.OrderService;
import com.orderflow.order.support.AbstractIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Tests for the guarantee the outbox exists to provide.
 *
 * <p>The claim being checked is narrow and specific: <em>a message is published
 * if and only if the state change that produced it was committed</em>. Both
 * directions matter. A message without its state change means another service
 * acts on something that never happened; a state change without its message
 * means a saga stalls forever with nobody waiting on it.
 */
@AbstractIntegrationTest.IntegrationTest
@DisplayName("Transactional outbox")
class OutboxIT extends AbstractIntegrationTest {

    @Autowired OrderService orderService;
    @Autowired OrderRepository orders;
    @Autowired OutboxWriter outboxWriter;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager txManager;

    @Test
    @DisplayName("The order and its messages commit together")
    void messagesCommitWithTheOrder() {
        OrderDtos.OrderResponse created = orderService.create(request("outbox-commit", "WIDGET-BLUE"));

        List<String> types = messageTypesFor(created.sagaId());
        assertThat(types)
                .as("creating an order must queue both the public fact and the first saga step")
                .containsExactlyInAnyOrder("OrderCreated", "ReserveInventory");

        // Ordering is by insertion, and the relay publishes in that order, so
        // OrderCreated cannot arrive after the command it precedes.
        assertThat(types.get(0)).isEqualTo("OrderCreated");
    }

    @Test
    @DisplayName("A rolled-back transaction leaves no message behind")
    void rollbackLeavesNoMessage() {
        String customer = "outbox-rollback-" + UUID.randomUUID();
        TransactionTemplate tx = new TransactionTemplate(txManager);

        // The order is created and then the surrounding transaction fails. This
        // is exactly the case that publishing to Kafka directly would get wrong:
        // the broker would already hold a ReserveInventory for an order that does
        // not exist, and inventory would set stock aside for it.
        assertThatThrownBy(() -> tx.execute(status -> {
            orderService.create(request(customer, "WIDGET-RED"));
            throw new IllegalStateException("simulated failure after the order was written");
        })).isInstanceOf(IllegalStateException.class);

        Integer orderCount = jdbc.queryForObject(
                "SELECT count(*) FROM orders WHERE customer_ref = ?", Integer.class, customer);
        Integer messageCount = jdbc.queryForObject(
                "SELECT count(*) FROM outbox_events e "
                        + "WHERE e.saga_id IN (SELECT saga_id FROM orders WHERE customer_ref = ?)",
                Integer.class, customer);

        assertThat(orderCount).isZero();
        assertThat(messageCount)
                .as("no order means no message; the two cannot disagree")
                .isZero();
    }

    @Test
    @DisplayName("Queuing a message outside a transaction is refused outright")
    void enqueueOutsideATransactionIsRefused() {
        // OutboxWriter declares MANDATORY propagation precisely so this fails.
        // If it committed on its own, the outbox would silently degrade into
        // "publish regardless of whether the state change succeeded" — the exact
        // bug it exists to prevent, and one that would only show up under load.
        assertThatThrownBy(() -> outboxWriter.enqueue(
                UUID.randomUUID(), "Order", "1", Topics.ORDER_EVENTS, "OrderCreated", "{}"))
                .isInstanceOf(org.springframework.transaction.IllegalTransactionStateException.class);
    }

    @Test
    @DisplayName("The relay publishes pending messages and marks them so")
    void relayPublishesAndMarks() {
        OrderDtos.OrderResponse created = orderService.create(request("outbox-relay", "GIZMO-SMALL"));

        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
            Integer pending = jdbc.queryForObject(
                    "SELECT count(*) FROM outbox_events WHERE saga_id = ? AND published_at IS NULL",
                    Integer.class, created.sagaId());
            assertThat(pending).as("the relay should drain the backlog for this saga").isZero();
        });

        Integer attempts = jdbc.queryForObject(
                "SELECT coalesce(max(attempts), 0) FROM outbox_events WHERE saga_id = ?",
                Integer.class, created.sagaId());
        assertThat(attempts)
                .as("a healthy broker should need no retries")
                .isZero();
    }

    private OrderDtos.CreateOrderRequest request(String customer, String sku) {
        return new OrderDtos.CreateOrderRequest(customer,
                List.of(new OrderDtos.LineRequest(sku, 2, 15_00L)));
    }

    private List<String> messageTypesFor(UUID sagaId) {
        return jdbc.queryForList(
                "SELECT message_type FROM outbox_events WHERE saga_id = ? ORDER BY id",
                String.class, sagaId);
    }
}
