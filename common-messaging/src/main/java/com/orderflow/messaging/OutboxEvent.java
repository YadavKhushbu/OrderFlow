package com.orderflow.messaging;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;
import java.util.UUID;

/**
 * A message waiting to be published, stored in the same database as the state
 * change that produced it.
 *
 * <p>This row is written inside the business transaction. If that transaction
 * rolls back, the message vanishes with it and was never sent. If it commits,
 * the message is durably queued and <em>will</em> be sent, however many times
 * the process crashes first. That equivalence is the entire value of the
 * pattern, and it cannot be achieved by publishing to Kafka from application
 * code — there is no way to make a database commit and a broker write succeed
 * or fail together.
 */
@Entity
@Table(name = "outbox_events")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Travels in a header; consumers deduplicate on it. */
    @Column(name = "message_id", nullable = false, unique = true)
    private UUID messageId;

    @Column(name = "saga_id", nullable = false)
    private UUID sagaId;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    @Column(nullable = false)
    private String topic;

    /**
     * Kafka partition key. Always the saga id, so every message of one saga lands
     * on one partition and is delivered in the order it was written. Keying by
     * anything else would let a compensation overtake the action it compensates.
     */
    @Column(name = "message_key", nullable = false)
    private String messageKey;

    @Column(name = "message_type", nullable = false)
    private String messageType;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    /** Null while pending. Set once the broker has acknowledged the write. */
    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(nullable = false)
    @Builder.Default
    private int attempts = 0;

    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;

    public boolean isPending() {
        return publishedAt == null;
    }
}
