package com.orderflow.order.domain;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "order_lines")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderLineItem {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "order_id")
    private Order order;

    @Column(nullable = false)
    private String sku;

    @Column(nullable = false)
    private int quantity;

    /** Price captured at order time; a later price change must not rewrite what was charged. */
    @Column(name = "unit_price_cents", nullable = false)
    private long unitPriceCents;
}
