package com.orderflow.payment;

import com.orderflow.events.PaymentCommand;
import com.orderflow.events.PaymentEvent;
import com.orderflow.events.Topics;
import com.orderflow.messaging.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Authorises and refunds payments for the order saga.
 *
 * <h2>The gateway is simulated, and deliberately not random</h2>
 *
 * <p>There is no real payment provider here. What matters for this project is
 * the <em>saga</em> behaviour around a decline, not the HTTP call that produces
 * one, so the outcome is decided by explicit rules: a customer reference
 * containing DECLINE always fails, and anything over the configured ceiling
 * fails as insufficient funds.
 *
 * <p>Deterministic rather than random on purpose. A random decline rate would
 * make the compensation test flaky and the demo unreproducible — and a flaky
 * test that guards the most important branch of the saga is worse than no test,
 * because it teaches people to rerun until green.
 *
 * <p>A real integration would replace {@link #callGateway} with a provider SDK
 * call, keep the same idempotency key, and wrap it in a circuit breaker so a
 * provider outage fails fast instead of exhausting the consumer thread pool.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);
    private static final String AGGREGATE = "Payment";

    private final PaymentRepository payments;
    private final OutboxWriter outbox;
    private final long declineAboveCents;

    public PaymentService(PaymentRepository payments,
                          OutboxWriter outbox,
                          @Value("${orderflow.payment.decline-above-cents:1000000}") long declineAboveCents) {
        this.payments = payments;
        this.outbox = outbox;
        this.declineAboveCents = declineAboveCents;
    }

    @Transactional
    public void authorize(PaymentCommand.Authorize command) {
        // The idempotency key, not the message id, is what makes this safe. A
        // command redelivered under a *different* message id would pass the
        // consumer-level dedup and still must not charge twice.
        Optional<Payment> existing = payments.findByIdempotencyKey(command.idempotencyKey());
        if (existing.isPresent()) {
            Payment payment = existing.get();
            log.debug("Payment for key {} already exists ({}); replying again",
                    command.idempotencyKey(), payment.getStatus());
            // Reply again: a missing reply is the most likely reason this command
            // came back at all.
            emitFor(payment);
            return;
        }

        GatewayResult result = callGateway(command);

        Payment payment = Payment.builder()
                .sagaId(command.sagaId())
                .orderId(command.orderId())
                .customerRef(command.customerRef())
                .amountCents(command.amountCents())
                .idempotencyKey(command.idempotencyKey())
                .status(result.approved() ? Payment.Status.AUTHORIZED : Payment.Status.DECLINED)
                .transactionRef(result.transactionRef())
                .declineCode(result.declineCode())
                .build();

        payments.saveAndFlush(payment);
        emitFor(payment);

        if (result.approved()) {
            log.info("Saga {} order {}: authorised {} cents as {}",
                    command.sagaId(), command.orderId(), command.amountCents(), result.transactionRef());
        } else {
            log.info("Saga {} order {}: declined ({})",
                    command.sagaId(), command.orderId(), result.declineCode());
        }
    }

    /**
     * The compensating action for a successful authorisation.
     *
     * <p>Not reachable from the current saga, which only compensates inventory,
     * but present because the saga's shape invites a third step — shipping, say —
     * after payment, and at that point money already taken has to be given back.
     */
    @Transactional
    public void refund(PaymentCommand.Refund command) {
        Optional<Payment> found = payments.findBySagaId(command.sagaId());
        if (found.isEmpty()) {
            log.warn("Refund requested for unknown saga {}; nothing to do", command.sagaId());
            return;
        }

        Payment payment = found.get();
        if (payment.getStatus() != Payment.Status.AUTHORIZED) {
            log.debug("Refund for saga {} ignored; payment is {}", command.sagaId(), payment.getStatus());
            return;
        }

        payment.setStatus(Payment.Status.REFUNDED);
        payment.setRefundedAt(Instant.now());
        payments.save(payment);

        outbox.enqueue(payment.getSagaId(), AGGREGATE, String.valueOf(payment.getOrderId()),
                Topics.PAYMENT_EVENTS, "PaymentRefunded",
                new PaymentEvent.Refunded(payment.getSagaId(), payment.getOrderId(),
                        payment.getTransactionRef(), Instant.now()));

        log.info("Saga {} order {}: refunded ({})", command.sagaId(), command.orderId(), command.reason());
    }

    private void emitFor(Payment payment) {
        if (payment.getStatus() == Payment.Status.DECLINED) {
            outbox.enqueue(payment.getSagaId(), AGGREGATE, String.valueOf(payment.getOrderId()),
                    Topics.PAYMENT_EVENTS, "PaymentFailed",
                    new PaymentEvent.Failed(payment.getSagaId(), payment.getOrderId(),
                            payment.getDeclineCode(), "Payment was declined by the provider", Instant.now()));
        } else {
            outbox.enqueue(payment.getSagaId(), AGGREGATE, String.valueOf(payment.getOrderId()),
                    Topics.PAYMENT_EVENTS, "PaymentAuthorized",
                    new PaymentEvent.Authorized(payment.getSagaId(), payment.getOrderId(),
                            payment.getTransactionRef(), payment.getAmountCents(), Instant.now()));
        }
    }

    /** Stands in for the provider call. See the class comment. */
    private GatewayResult callGateway(PaymentCommand.Authorize command) {
        if (command.customerRef() != null && command.customerRef().toUpperCase().contains("DECLINE")) {
            return GatewayResult.declined("CARD_DECLINED");
        }
        if (command.amountCents() > declineAboveCents) {
            return GatewayResult.declined("INSUFFICIENT_FUNDS");
        }
        return GatewayResult.approved("TXN-" + UUID.randomUUID().toString().substring(0, 12).toUpperCase());
    }

    private record GatewayResult(boolean approved, String transactionRef, String declineCode) {

        static GatewayResult approved(String transactionRef) {
            return new GatewayResult(true, transactionRef, null);
        }

        static GatewayResult declined(String declineCode) {
            return new GatewayResult(false, null, declineCode);
        }
    }
}
