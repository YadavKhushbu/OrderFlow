package com.orderflow.events;

/**
 * Every topic on the bus, named in one place.
 *
 * <p>Commands and events are separated deliberately, because they are different
 * kinds of message and confusing them is how event-driven systems turn into
 * distributed spaghetti:
 *
 * <ul>
 *   <li>A <b>command</b> is addressed to exactly one service and tells it to do
 *       something. It may be refused. {@code ReserveInventory} is a request.</li>
 *   <li>An <b>event</b> is a statement of fact, addressed to nobody in
 *       particular. {@code InventoryReserved} already happened and cannot be
 *       refused; any number of services may care, or none.</li>
 * </ul>
 *
 * <p>The practical consequence: a service owns its command topic and is the only
 * consumer of it, while anyone may subscribe to an event topic without the
 * publisher knowing or caring. Adding a new consumer to an event topic requires
 * no change to the service that emits it.
 */
public final class Topics {

    private Topics() {
    }

    /** Commands to the inventory service. Single consumer group. */
    public static final String INVENTORY_COMMANDS = "orderflow.commands.inventory";

    /** Commands to the payment service. Single consumer group. */
    public static final String PAYMENT_COMMANDS = "orderflow.commands.payment";

    /** Facts published by the inventory service. */
    public static final String INVENTORY_EVENTS = "orderflow.events.inventory";

    /** Facts published by the payment service. */
    public static final String PAYMENT_EVENTS = "orderflow.events.payment";

    /** Facts published by the order service, for downstream consumers. */
    public static final String ORDER_EVENTS = "orderflow.events.order";

    /**
     * Where messages go after retries are exhausted.
     *
     * <p>One shared dead-letter topic rather than one per source: a human being
     * has to look at these, and giving them five places to look means they look
     * at none of them. The original topic travels in a header.
     */
    public static final String DEAD_LETTER = "orderflow.dlt";
}
