package com.orderflow.events;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;
import java.util.UUID;

/**
 * Instructions to the inventory service.
 *
 * <p>Sealed, so the set of things that may be asked of inventory is closed and a
 * {@code switch} over it is checked by the compiler. When a new command is added
 * later, every handler that fails to account for it stops compiling — which is
 * exactly the behaviour you want in a system where the alternative is a message
 * being silently ignored in production.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = InventoryCommand.Reserve.class, name = "ReserveInventory"),
        @JsonSubTypes.Type(value = InventoryCommand.Release.class, name = "ReleaseInventory")
})
public sealed interface InventoryCommand {

    UUID sagaId();

    /** Take stock out of the available pool and hold it for this order. */
    record Reserve(UUID sagaId, Long orderId, List<OrderLine> lines) implements InventoryCommand {
    }

    /**
     * The compensating action for {@link Reserve}.
     *
     * <p>Sent when a later step of the saga fails — a declined payment, for
     * instance — and the stock already taken must go back. There is no rollback
     * across services, so undo has to be an explicit message like any other.
     */
    record Release(UUID sagaId, Long orderId, String reason) implements InventoryCommand {
    }
}
