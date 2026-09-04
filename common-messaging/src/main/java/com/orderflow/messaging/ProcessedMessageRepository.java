package com.orderflow.messaging;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, UUID> {

    boolean existsByMessageIdAndConsumer(UUID messageId, String consumer);
}
