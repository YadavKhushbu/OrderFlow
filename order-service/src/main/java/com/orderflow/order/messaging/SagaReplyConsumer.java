package com.orderflow.order.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.events.InventoryEvent;
import com.orderflow.events.PaymentEvent;
import com.orderflow.events.Topics;
import com.orderflow.messaging.IdempotentConsumer;
import com.orderflow.messaging.MessageIds;
import com.orderflow.messaging.UnparseableMessageException;
import com.orderflow.order.saga.OrderSaga;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Feeds saga replies from Kafka into {@link OrderSaga}.
 *
 * <p>Kept thin on purpose. Everything here is transport concern — headers,
 * deserialisation, deduplication, logging context — and none of it is business
 * logic. The saga itself never mentions Kafka, which is what lets it be tested
 * by calling methods rather than by standing up a broker.
 */
@Component
public class SagaReplyConsumer {

    private static final Logger log = LoggerFactory.getLogger(SagaReplyConsumer.class);

    private static final String INVENTORY_CONSUMER = "order-service:inventory-events";
    private static final String PAYMENT_CONSUMER = "order-service:payment-events";

    private final OrderSaga saga;
    private final IdempotentConsumer idempotent;
    private final ObjectMapper objectMapper;

    public SagaReplyConsumer(OrderSaga saga, IdempotentConsumer idempotent, ObjectMapper objectMapper) {
        this.saga = saga;
        this.idempotent = idempotent;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = Topics.INVENTORY_EVENTS, groupId = "${orderflow.consumer-group}")
    public void onInventoryEvent(ConsumerRecord<String, String> record) {
        consume(record, INVENTORY_CONSUMER, payload -> {
            InventoryEvent event = read(payload, InventoryEvent.class);
            saga.handle(event);
        });
    }

    @KafkaListener(topics = Topics.PAYMENT_EVENTS, groupId = "${orderflow.consumer-group}")
    public void onPaymentEvent(ConsumerRecord<String, String> record) {
        consume(record, PAYMENT_CONSUMER, payload -> {
            PaymentEvent event = read(payload, PaymentEvent.class);
            saga.handle(event);
        });
    }

    private void consume(ConsumerRecord<String, String> record, String consumer, PayloadHandler handler) {
        UUID messageId = MessageIds.from(record);

        // Putting the saga id in the MDC means every log line emitted while this
        // message is handled carries it, including lines from deep inside the
        // saga. Reconstructing one order's journey across services then costs a
        // single query rather than a manual join across timestamps.
        MDC.put("sagaId", MessageIds.sagaId(record));
        MDC.put("messageId", messageId.toString());
        try {
            idempotent.runOnce(messageId, consumer, () -> handler.handle(record.value()));
        } catch (IdempotentConsumer.DuplicateMessageException e) {
            // Lost a race with a concurrent delivery of the same message. The
            // winner's work stands, so there is nothing to do and nothing wrong.
            log.debug("Concurrent duplicate ignored: {}", e.getMessage());
        } finally {
            MDC.remove("sagaId");
            MDC.remove("messageId");
        }
    }

    private <T> T read(String payload, Class<T> type) {
        try {
            return objectMapper.readValue(payload, type);
        } catch (Exception e) {
            // Deliberately not retryable. A payload that cannot be parsed will
            // never parse, so retrying only delays the inevitable and blocks the
            // partition behind it. The error handler sends it straight to the
            // dead-letter topic.
            throw new UnparseableMessageException(payload, e);
        }
    }

    @FunctionalInterface
    private interface PayloadHandler {
        void handle(String payload);
    }

}
