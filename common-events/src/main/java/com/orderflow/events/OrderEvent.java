package com.orderflow.events;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Facts published by the order service for anyone downstream.
 *
 * <p>Nothing in this project consumes them; they exist because they are the
 * whole point of publishing events rather than making calls. A notifications
 * service, an analytics pipeline or a warehouse feed can subscribe later without
 * a single line changing here.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = OrderEvent.Created.class, name = "OrderCreated"),
        @JsonSubTypes.Type(value = OrderEvent.Confirmed.class, name = "OrderConfirmed"),
        @JsonSubTypes.Type(value = OrderEvent.Cancelled.class, name = "OrderCancelled")
})
public sealed interface OrderEvent {

    UUID sagaId();

    Long orderId();

    Instant occurredAt();

    record Created(UUID sagaId, Long orderId, String customerRef, long totalCents,
                   List<OrderLine> lines, Instant occurredAt) implements OrderEvent {
    }

    record Confirmed(UUID sagaId, Long orderId, String transactionRef, long totalCents, Instant occurredAt)
            implements OrderEvent {
    }

    /** @param reason why the saga could not complete, in terms a customer could be shown */
    record Cancelled(UUID sagaId, Long orderId, String reason, Instant occurredAt) implements OrderEvent {
    }
}
