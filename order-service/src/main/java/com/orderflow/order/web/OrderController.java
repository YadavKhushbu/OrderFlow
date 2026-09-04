package com.orderflow.order.web;

import com.orderflow.order.dto.OrderDtos;
import com.orderflow.order.saga.OrderService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriComponentsBuilder;

@RestController
@RequestMapping("/api/v1/orders")
@Tag(name = "Orders")
@Validated
public class OrderController {

    private final OrderService orders;

    public OrderController(OrderService orders) {
        this.orders = orders;
    }

    @PostMapping
    @Operation(summary = "Place an order and start its saga",
            description = """
                    Returns 202 Accepted with the order in PENDING. The response
                    does not mean the order is confirmed: inventory and payment
                    run asynchronously. Poll GET /api/v1/orders/{id} or consume
                    orderflow.events.order to learn the outcome.
                    """)
    public ResponseEntity<OrderDtos.OrderResponse> create(
            @Valid @RequestBody OrderDtos.CreateOrderRequest request,
            UriComponentsBuilder uriBuilder) {

        OrderDtos.OrderResponse created = orders.create(request);

        // 202, not 201. The order resource exists, but the work it represents has
        // only been accepted, and telling a client "Created" would invite them to
        // treat a PENDING order as a confirmed sale.
        return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .location(uriBuilder.path("/api/v1/orders/{id}").build(created.id()))
                .body(created);
    }

    @GetMapping("/{orderId}")
    @Operation(summary = "Fetch an order and its current saga status")
    public OrderDtos.OrderResponse get(@PathVariable Long orderId) {
        return orders.get(orderId);
    }

    @GetMapping
    @Operation(summary = "List a customer's orders, newest first")
    public OrderDtos.PageResponse<OrderDtos.OrderResponse> list(
            @RequestParam String customerRef,
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        return orders.listForCustomer(customerRef, page, size);
    }
}
