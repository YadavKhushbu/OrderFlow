package com.orderflow.inventory;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "reservation_lines")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ReservationLine {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "reservation_id")
    private Reservation reservation;

    @Column(nullable = false)
    private String sku;

    @Column(nullable = false)
    private int quantity;
}
