package com.orderflow.payment;

import com.orderflow.events.PaymentCommand;
import com.orderflow.payment.support.AbstractIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Authorisation, decline and refund.
 *
 * <p>The test that matters most is the duplicate one. Everything else here is
 * ordinary branching; charging a customer twice because a broker redelivered a
 * command is the single most expensive bug this service could have, and the
 * unique constraint on the idempotency key is what prevents it.
 */
@AbstractIntegrationTest.IntegrationTest
@DisplayName("Payment authorisation")
class PaymentServiceIT extends AbstractIntegrationTest {

    @Autowired PaymentService payments;
    @Autowired PaymentRepository repository;
    @Autowired JdbcTemplate jdbc;

    @Test
    @DisplayName("A normal charge is authorised and announced")
    void authorises() {
        UUID sagaId = UUID.randomUUID();

        payments.authorize(command(sagaId, 10L, "cust-ok", 5_000L));

        Payment payment = repository.findBySagaId(sagaId).orElseThrow();
        assertThat(payment.getStatus()).isEqualTo(Payment.Status.AUTHORIZED);
        assertThat(payment.getTransactionRef()).startsWith("TXN-");
        assertThat(messageTypes(sagaId)).containsExactly("PaymentAuthorized");
    }

    @Test
    @DisplayName("An amount over the ceiling is declined as insufficient funds")
    void declinesLargeAmounts() {
        UUID sagaId = UUID.randomUUID();

        payments.authorize(command(sagaId, 11L, "cust-rich", 99_999_999L));

        Payment payment = repository.findBySagaId(sagaId).orElseThrow();
        assertThat(payment.getStatus()).isEqualTo(Payment.Status.DECLINED);
        assertThat(payment.getDeclineCode()).isEqualTo("INSUFFICIENT_FUNDS");
        // A decline is a business outcome, not an error: the saga is told, and it
        // compensates. Throwing here would dead-letter the message instead and
        // leave the order stuck.
        assertThat(messageTypes(sagaId)).containsExactly("PaymentFailed");
    }

    @Test
    @DisplayName("The DECLINE marker forces a decline, so the failure path can be demonstrated")
    void declinesMarkedCustomers() {
        UUID sagaId = UUID.randomUUID();

        payments.authorize(command(sagaId, 12L, "cust-DECLINE-me", 1_000L));

        assertThat(repository.findBySagaId(sagaId).orElseThrow().getDeclineCode())
                .isEqualTo("CARD_DECLINED");
    }

    @Test
    @DisplayName("A redelivered command reuses the original payment instead of charging again")
    void doesNotChargeTwice() {
        UUID sagaId = UUID.randomUUID();
        PaymentCommand.Authorize command = command(sagaId, 13L, "cust-retry", 7_500L);

        payments.authorize(command);
        String firstRef = repository.findBySagaId(sagaId).orElseThrow().getTransactionRef();

        // Exactly what happens when a consumer crashes after processing but
        // before its offset is committed.
        payments.authorize(command);

        Integer rows = jdbc.queryForObject(
                "SELECT count(*) FROM payments WHERE saga_id = ?", Integer.class, sagaId);
        assertThat(rows).as("one command, however many deliveries, means one charge").isEqualTo(1);
        assertThat(repository.findBySagaId(sagaId).orElseThrow().getTransactionRef())
                .isEqualTo(firstRef);

        // But the reply goes out again, because a lost reply is the likeliest
        // reason the command came back.
        assertThat(messageTypes(sagaId)).containsExactly("PaymentAuthorized", "PaymentAuthorized");
    }

    @Test
    @DisplayName("Refunding an authorised payment marks it and announces the reversal")
    void refunds() {
        UUID sagaId = UUID.randomUUID();
        payments.authorize(command(sagaId, 14L, "cust-refund", 3_000L));

        payments.refund(new PaymentCommand.Refund(sagaId, 14L, "order cancelled downstream"));

        Payment payment = repository.findBySagaId(sagaId).orElseThrow();
        assertThat(payment.getStatus()).isEqualTo(Payment.Status.REFUNDED);
        assertThat(payment.getRefundedAt()).isNotNull();
        assertThat(messageTypes(sagaId)).containsExactly("PaymentAuthorized", "PaymentRefunded");
    }

    @Test
    @DisplayName("Refunding a declined payment does nothing")
    void doesNotRefundWhatWasNeverCharged() {
        UUID sagaId = UUID.randomUUID();
        payments.authorize(command(sagaId, 15L, "cust-DECLINE-again", 1_000L));

        payments.refund(new PaymentCommand.Refund(sagaId, 15L, "compensation"));

        assertThat(repository.findBySagaId(sagaId).orElseThrow().getStatus())
                .as("money that was never taken cannot be given back")
                .isEqualTo(Payment.Status.DECLINED);
        assertThat(messageTypes(sagaId)).doesNotContain("PaymentRefunded");
    }

    private PaymentCommand.Authorize command(UUID sagaId, Long orderId, String customer, long amount) {
        return new PaymentCommand.Authorize(sagaId, orderId, customer, amount,
                "order-" + orderId + "-" + sagaId);
    }

    private List<String> messageTypes(UUID sagaId) {
        return jdbc.queryForList(
                "SELECT message_type FROM outbox_events WHERE saga_id = ? ORDER BY id",
                String.class, sagaId);
    }
}
