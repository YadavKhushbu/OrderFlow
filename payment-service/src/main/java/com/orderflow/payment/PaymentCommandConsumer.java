package com.orderflow.payment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.events.PaymentCommand;
import com.orderflow.events.Topics;
import com.orderflow.messaging.IdempotentConsumer;
import com.orderflow.messaging.MessageIds;
import com.orderflow.messaging.UnparseableMessageException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class PaymentCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentCommandConsumer.class);
    private static final String CONSUMER = "payment-service:commands";

    private final PaymentService payments;
    private final IdempotentConsumer idempotent;
    private final ObjectMapper objectMapper;

    public PaymentCommandConsumer(PaymentService payments,
                                  IdempotentConsumer idempotent,
                                  ObjectMapper objectMapper) {
        this.payments = payments;
        this.idempotent = idempotent;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = Topics.PAYMENT_COMMANDS, groupId = "${orderflow.consumer-group}")
    public void onCommand(ConsumerRecord<String, String> record) {
        UUID messageId = MessageIds.from(record);
        MDC.put("sagaId", MessageIds.sagaId(record));
        try {
            idempotent.runOnce(messageId, CONSUMER, () -> dispatch(record.value()));
        } catch (IdempotentConsumer.DuplicateMessageException e) {
            log.debug("Concurrent duplicate ignored: {}", e.getMessage());
        } finally {
            MDC.remove("sagaId");
        }
    }

    private void dispatch(String payload) {
        PaymentCommand command;
        try {
            command = objectMapper.readValue(payload, PaymentCommand.class);
        } catch (Exception e) {
            throw new UnparseableMessageException(payload, e);
        }

        if (command instanceof PaymentCommand.Authorize authorize) {
            payments.authorize(authorize);
        } else if (command instanceof PaymentCommand.Refund refund) {
            payments.refund(refund);
        } else {
            throw new IllegalStateException("Unhandled payment command: " + command.getClass());
        }
    }
}
