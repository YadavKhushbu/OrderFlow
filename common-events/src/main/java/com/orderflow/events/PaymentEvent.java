package com.orderflow.events;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Instant;
import java.util.UUID;

/** Facts published by the payment service. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PaymentEvent.Authorized.class, name = "PaymentAuthorized"),
        @JsonSubTypes.Type(value = PaymentEvent.Failed.class, name = "PaymentFailed"),
        @JsonSubTypes.Type(value = PaymentEvent.Refunded.class, name = "PaymentRefunded")
})
public sealed interface PaymentEvent {

    UUID sagaId();

    Long orderId();

    Instant occurredAt();

    record Authorized(UUID sagaId, Long orderId, String transactionRef, long amountCents, Instant occurredAt)
            implements PaymentEvent {
    }

    /**
     * @param declineCode provider-style code such as INSUFFICIENT_FUNDS. Kept
     *                    distinct from the human message so the saga can branch
     *                    on it without string-matching prose.
     */
    record Failed(UUID sagaId, Long orderId, String declineCode, String reason, Instant occurredAt)
            implements PaymentEvent {
    }

    record Refunded(UUID sagaId, Long orderId, String transactionRef, Instant occurredAt)
            implements PaymentEvent {
    }
}
