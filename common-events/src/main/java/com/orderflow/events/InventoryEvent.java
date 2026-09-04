package com.orderflow.events;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Instant;
import java.util.UUID;

/** Facts published by the inventory service. Statements about what already happened. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = InventoryEvent.Reserved.class, name = "InventoryReserved"),
        @JsonSubTypes.Type(value = InventoryEvent.ReservationFailed.class, name = "InventoryReservationFailed"),
        @JsonSubTypes.Type(value = InventoryEvent.Released.class, name = "InventoryReleased")
})
public sealed interface InventoryEvent {

    UUID sagaId();

    Long orderId();

    Instant occurredAt();

    record Reserved(UUID sagaId, Long orderId, String reservationRef, Instant occurredAt)
            implements InventoryEvent {
    }

    /**
     * @param reason human-readable, for support and logs
     * @param sku    the first SKU that could not be satisfied, or null if the
     *               failure was not about a specific item
     */
    record ReservationFailed(UUID sagaId, Long orderId, String reason, String sku, Instant occurredAt)
            implements InventoryEvent {
    }

    record Released(UUID sagaId, Long orderId, Instant occurredAt) implements InventoryEvent {
    }
}
