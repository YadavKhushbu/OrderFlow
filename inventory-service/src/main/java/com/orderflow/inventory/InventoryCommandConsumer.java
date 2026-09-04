package com.orderflow.inventory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.events.InventoryCommand;
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
public class InventoryCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandConsumer.class);
    private static final String CONSUMER = "inventory-service:commands";

    private final InventoryService inventory;
    private final IdempotentConsumer idempotent;
    private final ObjectMapper objectMapper;

    public InventoryCommandConsumer(InventoryService inventory,
                                    IdempotentConsumer idempotent,
                                    ObjectMapper objectMapper) {
        this.inventory = inventory;
        this.idempotent = idempotent;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = Topics.INVENTORY_COMMANDS, groupId = "${orderflow.consumer-group}")
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
        InventoryCommand command;
        try {
            command = objectMapper.readValue(payload, InventoryCommand.class);
        } catch (Exception e) {
            throw new UnparseableMessageException(payload, e);
        }

        // Exhaustive over a sealed interface: adding a third command makes this
        // fail to compile rather than silently ignoring it at runtime.
        if (command instanceof InventoryCommand.Reserve reserve) {
            inventory.reserve(reserve);
        } else if (command instanceof InventoryCommand.Release release) {
            inventory.release(release);
        } else {
            throw new IllegalStateException("Unhandled inventory command: " + command.getClass());
        }
    }
}
