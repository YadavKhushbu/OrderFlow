package com.orderflow.messaging;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Runs a message handler at most once per message, per consumer.
 *
 * <p>Redelivery is normal, not exceptional. Kafka commits offsets separately
 * from the work, so a crash between the two replays whatever was in flight; a
 * consumer-group rebalance can hand the same records to a different instance;
 * and the outbox relay itself republishes anything it sent but failed to mark.
 * A handler that is not idempotent will eventually double-apply, and the day it
 * happens is the day traffic is highest.
 *
 * <p>The dedup record and the work share one transaction. That is what makes
 * this safe rather than merely likely to work: the guard cannot commit for work
 * that rolled back, and the work cannot commit without the guard.
 */
@Component
public class IdempotentConsumer {

    private static final Logger log = LoggerFactory.getLogger(IdempotentConsumer.class);

    private final ProcessedMessageRepository processed;
    private final Counter handled;
    private final Counter duplicates;

    public IdempotentConsumer(ProcessedMessageRepository processed, MeterRegistry metrics) {
        this.processed = processed;
        this.handled = Counter.builder("orderflow.messages").tag("outcome", "handled").register(metrics);
        // A non-zero duplicate rate is healthy and expected. A sudden spike
        // usually means consumers are timing out and being rebalanced.
        this.duplicates = Counter.builder("orderflow.messages").tag("outcome", "duplicate").register(metrics);
    }

    /**
     * @param messageId from the message header; if absent the caller must
     *                  synthesise a stable one rather than pass a random value,
     *                  which would defeat deduplication entirely
     * @return true if the action ran, false if it was a duplicate
     */
    @Transactional
    public boolean runOnce(UUID messageId, String consumer, Runnable action) {
        if (processed.existsByMessageIdAndConsumer(messageId, consumer)) {
            duplicates.increment();
            log.debug("Skipping duplicate message {} for consumer {}", messageId, consumer);
            return false;
        }

        action.run();

        try {
            processed.saveAndFlush(ProcessedMessage.builder()
                    .messageId(messageId)
                    .consumer(consumer)
                    .build());
        } catch (DataIntegrityViolationException e) {
            // Two deliveries of the same message were processed concurrently and
            // this one lost. Rolling back is correct: the winner's work stands,
            // and this transaction's duplicate work is discarded with it.
            duplicates.increment();
            log.debug("Concurrent duplicate of message {} for consumer {}", messageId, consumer);
            throw new DuplicateMessageException(messageId);
        }

        handled.increment();
        return true;
    }

    /** Signals a concurrent duplicate so the surrounding transaction rolls back. */
    public static class DuplicateMessageException extends RuntimeException {
        public DuplicateMessageException(UUID messageId) {
            super("Message " + messageId + " was processed concurrently by another delivery");
        }
    }
}
