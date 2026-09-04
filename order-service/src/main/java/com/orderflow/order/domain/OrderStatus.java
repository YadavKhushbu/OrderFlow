package com.orderflow.order.domain;

/**
 * Where an order sits in the saga.
 *
 * <p>These are saga states, not just labels. Each one records how far the
 * distributed flow got, which is what makes recovery possible: an instance that
 * restarts mid-saga can read the status and know what has already been done to
 * other services and therefore what still needs undoing.
 *
 * <pre>
 *   PENDING ──InventoryReserved──▶ INVENTORY_RESERVED ──PaymentAuthorized──▶ CONFIRMED
 *      │                                   │
 *      │ReservationFailed                  │PaymentFailed
 *      ▼                                   ▼
 *   CANCELLED ◀──InventoryReleased── COMPENSATING
 * </pre>
 */
public enum OrderStatus {

    /** Accepted and durable, but nothing has been reserved or charged yet. */
    PENDING,

    /** Stock is held. Something now exists in another service that must be undone if we fail. */
    INVENTORY_RESERVED,

    /** Paid and committed. Terminal, and the only successful outcome. */
    CONFIRMED,

    /** A step failed after side effects existed elsewhere; compensations are in flight. */
    COMPENSATING,

    /** Terminal failure. Every side effect has been undone. */
    CANCELLED;

    public boolean isTerminal() {
        return this == CONFIRMED || this == CANCELLED;
    }
}
