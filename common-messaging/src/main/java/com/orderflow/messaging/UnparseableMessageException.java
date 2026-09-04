package com.orderflow.messaging;

/**
 * A message whose payload will never deserialise.
 *
 * <p>Separated from ordinary failures because it must not be retried. A
 * malformed payload is malformed permanently, so retrying it burns the backoff
 * window and — far worse — blocks every message behind it on that partition
 * while it does. Messages carrying this exception go straight to the
 * dead-letter topic.
 */
public class UnparseableMessageException extends RuntimeException {

    public UnparseableMessageException(String payload, Throwable cause) {
        super("Could not deserialise message payload: "
                + (payload != null && payload.length() > 200 ? payload.substring(0, 200) + "..." : payload), cause);
    }
}
