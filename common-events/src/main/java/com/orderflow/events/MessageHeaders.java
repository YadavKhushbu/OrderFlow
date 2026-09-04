package com.orderflow.events;

/**
 * Header names carried on every message.
 *
 * <p>These live in headers rather than in the payload so infrastructure can read
 * them without deserialising the body: a dead-letter inspector, a log appender
 * or a router should not need to know the message schema to tell you which saga
 * a stuck message belongs to.
 */
public final class MessageHeaders {

    private MessageHeaders() {
    }

    /**
     * Unique per message. The basis of consumer-side deduplication.
     *
     * <p>Kafka delivers at least once. A consumer that commits its offset after
     * processing will reprocess anything that was in flight when it crashed, and
     * a rebalance can redeliver to a different instance. Every consumer here
     * therefore records the ids it has already handled.
     */
    public static final String MESSAGE_ID = "orderflow-message-id";

    /** The saga this message belongs to. Ties an entire distributed flow together in logs. */
    public static final String SAGA_ID = "orderflow-saga-id";

    /** Logical message type, so a consumer can route without full deserialisation. */
    public static final String MESSAGE_TYPE = "orderflow-message-type";

    /** Set by the error handler when a message is dead-lettered. */
    public static final String DLT_ORIGINAL_TOPIC = "orderflow-dlt-original-topic";

    /** Why it was dead-lettered, so triage does not start with a stack trace hunt. */
    public static final String DLT_REASON = "orderflow-dlt-reason";
}
