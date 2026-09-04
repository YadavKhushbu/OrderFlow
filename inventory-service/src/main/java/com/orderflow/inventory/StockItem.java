package com.orderflow.inventory;

import jakarta.persistence.*;
import lombok.*;

/**
 * Available stock for one SKU.
 *
 * <p>{@code reserved} is tracked separately from {@code onHand} rather than
 * simply decrementing a single number. The distinction matters because reserved
 * stock is physically still in the warehouse — it has been promised, not
 * shipped. Collapsing the two would make it impossible to answer "how much do we
 * actually have?" versus "how much can we still sell?", and a compensating
 * release would be indistinguishable from a restock.
 */
@Entity
@Table(name = "stock_items")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StockItem {

    @Id
    private String sku;

    @Column(name = "on_hand", nullable = false)
    private int onHand;

    @Column(nullable = false)
    private int reserved;

    /**
     * Optimistic locking rather than a row lock.
     *
     * <p>Two orders for the same SKU do collide, but rarely, and the work is a
     * few milliseconds long. Taking a pessimistic lock on every reservation
     * would serialise every order touching a popular product; letting the loser
     * fail and retry costs less than making the common case wait.
     */
    @Version
    @Column(nullable = false)
    private Long version;

    public int available() {
        return onHand - reserved;
    }

    public boolean canReserve(int quantity) {
        return available() >= quantity;
    }

    public void reserve(int quantity) {
        if (!canReserve(quantity)) {
            throw new IllegalStateException(
                    "Cannot reserve " + quantity + " of " + sku + "; only " + available() + " available");
        }
        reserved += quantity;
    }

    /**
     * Returns previously reserved stock to the sellable pool.
     *
     * <p>Clamped at zero rather than allowed to go negative. A duplicate release
     * — entirely possible given at-least-once delivery — must not corrupt the
     * count, and a negative reservation would silently inflate availability and
     * cause overselling later.
     */
    public void release(int quantity) {
        reserved = Math.max(0, reserved - quantity);
    }
}
