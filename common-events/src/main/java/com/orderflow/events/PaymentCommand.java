package com.orderflow.events;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.UUID;

/** Instructions to the payment service. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PaymentCommand.Authorize.class, name = "AuthorizePayment"),
        @JsonSubTypes.Type(value = PaymentCommand.Refund.class, name = "RefundPayment")
})
public sealed interface PaymentCommand {

    UUID sagaId();

    /**
     * @param idempotencyKey passed straight through to the payment provider.
     *                       Kafka redelivery is routine, and a payment that is
     *                       charged twice because a consumer retried is the most
     *                       expensive possible bug in this system.
     */
    record Authorize(UUID sagaId, Long orderId, String customerRef, long amountCents, String idempotencyKey)
            implements PaymentCommand {
    }

    /** The compensating action for {@link Authorize}. */
    record Refund(UUID sagaId, Long orderId, String reason) implements PaymentCommand {
    }
}
