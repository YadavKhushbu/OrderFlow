package com.orderflow.messaging;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;
import java.util.UUID;

/**
 * A record that this service has already handled a given message.
 *
 * <p>Written in the same transaction as whatever the message caused. Either both
 * land or neither does, so there is no window in which a message counts as
 * processed but its effect was rolled back — or worse, where the effect
 * committed and the record did not, leaving a redelivery free to apply it twice.
 *
 * <p>Uniqueness is on (message_id, consumer) rather than message_id alone,
 * because one message may legitimately be consumed by several independent
 * handlers.
 */
@Entity
@Table(name = "processed_messages",
        uniqueConstraints = @UniqueConstraint(name = "ux_processed_message_consumer",
                columnNames = {"message_id", "consumer"}))
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProcessedMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "message_id", nullable = false)
    private UUID messageId;

    @Column(nullable = false)
    private String consumer;

    @CreationTimestamp
    @Column(name = "processed_at", nullable = false, updatable = false)
    private Instant processedAt;
}
