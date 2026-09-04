package com.orderflow.events;

/**
 * One line of an order, as it travels between services.
 *
 * @param sku        the product identifier; the inventory service's primary key
 * @param quantity   units ordered, always positive
 * @param unitPriceCents price per unit in minor units
 */
public record OrderLine(String sku, int quantity, long unitPriceCents) {

    public OrderLine {
        if (sku == null || sku.isBlank()) {
            throw new IllegalArgumentException("sku is required");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive, got " + quantity);
        }
        if (unitPriceCents < 0) {
            throw new IllegalArgumentException("unitPriceCents cannot be negative");
        }
    }

    public long lineTotalCents() {
        return unitPriceCents * quantity;
    }
}
