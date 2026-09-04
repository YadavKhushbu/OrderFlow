package com.orderflow.payment;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payments")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Payment {

    public enum Status {
        AUTHORIZED,
        DECLINED,
        REFUNDED
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "saga_id", nullable = false, unique = true)
    private UUID sagaId;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(name = "customer_ref", nullable = false)
    private String customerRef;

    @Column(name = "amount_cents", nullable = false)
    private long amountCents;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status;

    @Column(name = "transaction_ref")
    private String transactionRef;

    @Column(name = "decline_code")
    private String declineCode;

    /**
     * The key sent to the payment provider, unique in this table.
     *
     * <p>This constraint is the last line of defence against charging a customer
     * twice. Delivery is at-least-once, so an {@code AuthorizePayment} command
     * genuinely can arrive more than once; if every other guard failed, the
     * database still refuses the second insert. Of all the duplicate-prevention
     * in this system, this is the one whose failure costs real money.
     */
    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "refunded_at")
    private Instant refundedAt;
}
