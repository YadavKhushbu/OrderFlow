package com.orderflow.messaging;

import com.orderflow.events.MessageHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** Reads the correlation headers off an incoming record. */
public final class MessageIds {

    private MessageIds() {
    }

    /**
     * The message's deduplication id.
     *
     * <p>Falls back to a value derived from the record's coordinates when the
     * header is missing — for instance a message published by a tool rather than
     * by the outbox relay. Deriving from topic/partition/offset keeps the id
     * <em>stable</em> across redeliveries of that same record, which a random
     * UUID would not: a random fallback would make every redelivery look like a
     * new message and silently disable deduplication for exactly the messages
     * whose provenance is least trustworthy.
     */
    public static UUID from(ConsumerRecord<String, String> record) {
        String header = headerValue(record, MessageHeaders.MESSAGE_ID);
        if (header != null) {
            try {
                return UUID.fromString(header);
            } catch (IllegalArgumentException ignored) {
                // Malformed header; fall through to the derived id.
            }
        }
        return UUID.nameUUIDFromBytes(
                (record.topic() + ":" + record.partition() + ":" + record.offset())
                        .getBytes(StandardCharsets.UTF_8));
    }

    public static String sagaId(ConsumerRecord<String, String> record) {
        String header = headerValue(record, MessageHeaders.SAGA_ID);
        return header != null ? header : record.key();
    }

    private static String headerValue(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
