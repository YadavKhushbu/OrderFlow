package com.orderflow.order.domain;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Correlates every message belonging to this order's saga.
     *
     * <p>Also the Kafka message key throughout, which is what keeps a saga's
     * messages on one partition and therefore in order relative to each other.
     */
    @Column(name = "saga_id", nullable = false, unique = true)
    private UUID sagaId;

    @Column(name = "customer_ref", nullable = false)
    private String customerRef;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Builder.Default
    private OrderStatus status = OrderStatus.PENDING;

    @Column(name = "total_cents", nullable = false)
    private long totalCents;

    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "payment_ref")
    private String paymentRef;

    @Column(name = "reservation_ref")
    private String reservationRef;

    /**
     * Guards against two saga replies being applied to the same order at once.
     *
     * <p>Replies arrive from independent consumers, and nothing stops a delayed
     * {@code PaymentFailed} landing at the same moment as a retried
     * {@code InventoryReserved}. Optimistic locking makes the loser fail loudly
     * and retry against fresh state, rather than overwriting a status transition
     * it never saw.
     */
    @Version
    @Column(nullable = false)
    private Long version;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    @Builder.Default
    private List<OrderLineItem> lines = new ArrayList<>();

    public void addLine(String sku, int quantity, long unitPriceCents) {
        lines.add(OrderLineItem.builder()
                .order(this)
                .sku(sku)
                .quantity(quantity)
                .unitPriceCents(unitPriceCents)
                .build());
    }

    public long recalculateTotal() {
        this.totalCents = lines.stream()
                .mapToLong(line -> line.getUnitPriceCents() * line.getQuantity())
                .sum();
        return this.totalCents;
    }
}
