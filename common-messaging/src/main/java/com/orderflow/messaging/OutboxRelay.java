package com.orderflow.messaging;

import com.orderflow.events.MessageHeaders;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Moves queued messages from the outbox table onto Kafka.
 *
 * <h2>Delivery semantics, stated honestly</h2>
 *
 * <p>This relay is <b>at-least-once</b>, and cannot be anything else. The
 * failure window is unavoidable: the broker acknowledges a message, and the
 * process dies before the row is marked published. On restart the message is
 * still pending and goes out a second time. No amount of care removes that
 * window — it is two systems with no shared transaction.
 *
 * <p>So the duplicate is not prevented, it is made harmless. Every message
 * carries a stable {@code messageId} and every consumer records the ids it has
 * applied. At-least-once delivery plus idempotent consumers gives exactly-once
 * <em>effect</em>, which is the only form of exactly-once that actually exists
 * in a distributed system.
 *
 * <h2>Why polling</h2>
 *
 * <p>A production system at scale would tail the write-ahead log with Debezium
 * instead, avoiding both the poll interval and the load of repeatedly scanning
 * for pending rows. Polling is chosen here because it is self-contained and its
 * failure modes are visible in one file; the partial index on
 * {@code (id) WHERE published_at IS NULL} keeps the scan proportional to the
 * backlog rather than to history.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final int batchSize;
    private final long sendTimeoutSeconds;

    private final Counter published;
    private final Counter failed;

    public OutboxRelay(OutboxRepository outbox,
                       KafkaTemplate<String, String> kafka,
                       MeterRegistry metrics,
                       @Value("${orderflow.outbox.batch-size:100}") int batchSize,
                       @Value("${orderflow.outbox.send-timeout-seconds:10}") long sendTimeoutSeconds) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.batchSize = batchSize;
        this.sendTimeoutSeconds = sendTimeoutSeconds;
        this.published = Counter.builder("orderflow.outbox.messages").tag("outcome", "published").register(metrics);
        this.failed = Counter.builder("orderflow.outbox.messages").tag("outcome", "failed").register(metrics);

        // Backlog depth is the single most useful alert on this component: it
        // rises whether Kafka is unreachable, the relay has stopped, or
        // production simply outpaces publication.
        metrics.gauge("orderflow.outbox.pending", outbox, OutboxRepository::countByPublishedAtIsNull);
    }

    /**
     * ShedLock keeps one instance publishing at a time.
     *
     * <p>Without it, every replica would read the same pending batch and publish
     * every message N times. That is survivable thanks to consumer
     * deduplication, but it multiplies broker traffic by the replica count for no
     * benefit whatsoever.
     */
    @Scheduled(fixedDelayString = "${orderflow.outbox.poll-interval:PT0.5S}")
    @SchedulerLock(name = "orderflow-outbox-relay", lockAtLeastFor = "PT0.2S", lockAtMostFor = "PT1M")
    @Transactional
    public void publishPending() {
        List<OutboxEvent> batch = outbox.findPendingBatch(PageRequest.of(0, batchSize));
        if (batch.isEmpty()) {
            return;
        }

        for (OutboxEvent event : batch) {
            try {
                // Synchronous on purpose. Publishing the batch asynchronously
                // would let message 7 reach the broker before message 3, undoing
                // the ordering the id-ordered query just established.
                kafka.send(toRecord(event)).get(sendTimeoutSeconds, TimeUnit.SECONDS);

                event.setPublishedAt(Instant.now());
                event.setLastError(null);
                published.increment();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Outbox relay interrupted; {} messages left for the next run", batch.size());
                break;
            } catch (Exception e) {
                event.setAttempts(event.getAttempts() + 1);
                event.setLastError(truncate(e.toString()));
                failed.increment();
                log.error("Failed to publish outbox message {} (attempt {})",
                        event.getMessageId(), event.getAttempts(), e);

                // Stop the batch rather than skipping ahead. Publishing later
                // messages past a failed one would reorder the stream, and if the
                // broker is unreachable the rest will fail anyway.
                break;
            }
        }
        outbox.saveAll(batch);
    }

    private ProducerRecord<String, String> toRecord(OutboxEvent event) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(event.getTopic(), event.getMessageKey(), event.getPayload());

        // Headers, not payload fields: infrastructure can read these without
        // knowing the message schema.
        record.headers().add(new RecordHeader(MessageHeaders.MESSAGE_ID,
                event.getMessageId().toString().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(MessageHeaders.SAGA_ID,
                event.getSagaId().toString().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(MessageHeaders.MESSAGE_TYPE,
                event.getMessageType().getBytes(StandardCharsets.UTF_8)));
        return record;
    }

    private String truncate(String message) {
        return message.length() <= 1000 ? message : message.substring(0, 1000);
    }
}
