package com.orderflow.order.saga;

import com.orderflow.events.InventoryCommand;
import com.orderflow.events.OrderEvent;
import com.orderflow.events.OrderLine;
import com.orderflow.events.Topics;
import com.orderflow.order.domain.Order;
import com.orderflow.order.domain.OrderLineItem;
import com.orderflow.order.domain.OrderStatus;
import com.orderflow.order.dto.OrderDtos;
import com.orderflow.messaging.OutboxWriter;
import com.orderflow.order.repo.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Accepts orders and starts their saga.
 *
 * <p>Creation returns as soon as the order is durable, without waiting for stock
 * or payment. That is a deliberate API contract, not a shortcut: making the
 * caller wait would couple its response time to two other services and its
 * availability to theirs, which is most of what event-driven design exists to
 * avoid. The client is told the order is PENDING and learns the outcome by
 * polling or by consuming order events.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private static final String AGGREGATE = "Order";

    private final OrderRepository orders;
    private final OutboxWriter outbox;

    public OrderService(OrderRepository orders, OutboxWriter outbox) {
        this.orders = orders;
        this.outbox = outbox;
    }

    /**
     * Persists the order and queues the first saga step atomically.
     *
     * <p>The order row and both outbox rows commit together. There is no state in
     * which an order exists but its saga was never started, and none in which a
     * reservation is requested for an order that does not exist.
     */
    @Transactional
    public OrderDtos.OrderResponse create(OrderDtos.CreateOrderRequest request) {
        UUID sagaId = UUID.randomUUID();

        Order order = Order.builder()
                .sagaId(sagaId)
                .customerRef(request.customerRef().trim())
                .status(OrderStatus.PENDING)
                .build();

        request.lines().forEach(line -> order.addLine(line.sku().trim(), line.quantity(), line.unitPriceCents()));
        order.recalculateTotal();

        orders.saveAndFlush(order);

        List<OrderLine> eventLines = order.getLines().stream()
                .map(line -> new OrderLine(line.getSku(), line.getQuantity(), line.getUnitPriceCents()))
                .toList();

        // A fact, for anyone downstream who cares.
        outbox.enqueue(sagaId, AGGREGATE, String.valueOf(order.getId()),
                Topics.ORDER_EVENTS, "OrderCreated",
                new OrderEvent.Created(sagaId, order.getId(), order.getCustomerRef(),
                        order.getTotalCents(), eventLines, Instant.now()));

        // And the first step of the saga. Enqueued after OrderCreated so the two
        // reach the bus in that order, which matters to anything replaying the
        // stream to rebuild state.
        outbox.enqueue(sagaId, AGGREGATE, String.valueOf(order.getId()),
                Topics.INVENTORY_COMMANDS, "ReserveInventory",
                new InventoryCommand.Reserve(sagaId, order.getId(), eventLines));

        log.info("Saga {} order {}: created for {} cents, requesting inventory",
                sagaId, order.getId(), order.getTotalCents());

        return toResponse(order);
    }

    @Transactional(readOnly = true)
    public OrderDtos.OrderResponse get(Long orderId) {
        return orders.findWithLinesById(orderId)
                .map(this::toResponse)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    @Transactional(readOnly = true)
    public OrderDtos.PageResponse<OrderDtos.OrderResponse> listForCustomer(String customerRef, int page, int size) {
        Page<Order> found = orders.findByCustomer(customerRef, PageRequest.of(page, size));
        return new OrderDtos.PageResponse<>(
                found.getContent().stream().map(this::toResponse).toList(),
                found.getNumber(), found.getSize(), found.getTotalElements(), found.getTotalPages());
    }

    private OrderDtos.OrderResponse toResponse(Order order) {
        List<OrderDtos.LineResponse> lines = order.getLines().stream()
                .map(this::toLineResponse)
                .toList();

        return new OrderDtos.OrderResponse(
                order.getId(),
                order.getSagaId(),
                order.getCustomerRef(),
                order.getStatus().name(),
                order.getTotalCents(),
                order.getFailureReason(),
                order.getPaymentRef(),
                order.getReservationRef(),
                order.getCreatedAt(),
                order.getUpdatedAt(),
                lines);
    }

    private OrderDtos.LineResponse toLineResponse(OrderLineItem line) {
        return new OrderDtos.LineResponse(
                line.getSku(), line.getQuantity(), line.getUnitPriceCents(),
                line.getUnitPriceCents() * line.getQuantity());
    }

    public static class OrderNotFoundException extends RuntimeException {
        public OrderNotFoundException(Long id) {
            super("Order " + id + " was not found");
        }
    }
}
