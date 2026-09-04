package com.orderflow.order.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public final class OrderDtos {

    private OrderDtos() {
    }

    public record CreateOrderRequest(
            @NotBlank @Size(max = 64) String customerRef,

            @NotEmpty(message = "an order needs at least one line")
            @Size(max = 50, message = "at most 50 lines per order")
            List<@Valid LineRequest> lines) {
    }

    public record LineRequest(
            @NotBlank @Size(max = 64) String sku,
            @Positive int quantity,
            @PositiveOrZero long unitPriceCents) {
    }

    /**
     * @param status one of PENDING, INVENTORY_RESERVED, CONFIRMED, COMPENSATING,
     *               CANCELLED. A freshly created order is always PENDING: the
     *               saga has not run yet, and the API deliberately does not wait
     *               for it. Poll this endpoint or consume the order events.
     */
    public record OrderResponse(
            Long id,
            UUID sagaId,
            String customerRef,
            String status,
            long totalCents,
            String failureReason,
            String paymentRef,
            String reservationRef,
            Instant createdAt,
            Instant updatedAt,
            List<LineResponse> lines) {
    }

    public record LineResponse(String sku, int quantity, long unitPriceCents, long lineTotalCents) {
    }

    public record PageResponse<T>(List<T> content, int page, int size, long totalElements, int totalPages) {
    }

    public record ApiError(Instant timestamp, int status, String code, String message,
                           String path, List<FieldViolation> violations) {
    }

    public record FieldViolation(String field, String message) {
    }
}
