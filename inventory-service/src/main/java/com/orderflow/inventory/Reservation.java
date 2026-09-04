package com.orderflow.inventory;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Stock held on behalf of one saga.
 *
 * <p>Recording the reservation as its own row, rather than only bumping counters
 * on {@link StockItem}, is what makes compensation possible. When a release
 * arrives minutes later the command says only which saga failed; without this
 * record there would be no way to know what to give back or whether it was
 * already given back.
 */
@Entity
@Table(name = "reservations")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Reservation {

    public enum Status {
        /** Stock is set aside and the saga is still running. */
        HELD,
        /** Compensated: the stock went back to the sellable pool. */
        RELEASED,
        /** The order was confirmed; the hold became a sale. */
        COMMITTED
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "saga_id", nullable = false, unique = true)
    private UUID sagaId;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(name = "reservation_ref", nullable = false)
    private String reservationRef;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Builder.Default
    private Status status = Status.HELD;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "released_at")
    private Instant releasedAt;

    @OneToMany(mappedBy = "reservation", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    @Builder.Default
    private List<ReservationLine> lines = new ArrayList<>();

    public void addLine(String sku, int quantity) {
        lines.add(ReservationLine.builder().reservation(this).sku(sku).quantity(quantity).build());
    }
}
