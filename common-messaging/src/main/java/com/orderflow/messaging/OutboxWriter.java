package com.orderflow.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Queues a message for publication as part of the caller's transaction.
 *
 * <p>Callers must already be inside a transaction, which is why every method
 * here is {@code MANDATORY} rather than {@code REQUIRED}. That choice is the
 * point of the class: if someone later calls this from outside a transaction,
 * the outbox row would commit on its own and the guarantee would quietly become
 * "the message is sent whether or not the state change succeeded" — the exact
 * bug the outbox exists to prevent. {@code MANDATORY} turns that mistake into an
 * immediate exception instead of a rare production inconsistency.
 */
@Component
public class OutboxWriter {

    private final OutboxRepository outbox;
    private final ObjectMapper objectMapper;

    public OutboxWriter(OutboxRepository outbox, ObjectMapper objectMapper) {
        this.outbox = outbox;
        this.objectMapper = objectMapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public OutboxEvent enqueue(UUID sagaId,
                               String aggregateType,
                               String aggregateId,
                               String topic,
                               String messageType,
                               Object payload) {

        OutboxEvent event = OutboxEvent.builder()
                .messageId(UUID.randomUUID())
                .sagaId(sagaId)
                .aggregateType(aggregateType)
                .aggregateId(aggregateId)
                .topic(topic)
                // Keyed by saga, always. Partition affinity is what preserves
                // ordering within one saga while leaving different sagas free to
                // be processed in parallel across partitions.
                .messageKey(sagaId.toString())
                .messageType(messageType)
                .payload(serialise(payload))
                .build();

        return outbox.save(event);
    }

    private String serialise(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            // Rolls back the business transaction too, which is correct: a state
            // change whose message cannot be written must not be committed alone.
            throw new IllegalStateException("Outbox payload could not be serialised", e);
        }
    }
}
