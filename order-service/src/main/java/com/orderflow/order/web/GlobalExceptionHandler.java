package com.orderflow.order.web;

import com.orderflow.order.dto.OrderDtos;
import com.orderflow.order.saga.OrderService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.List;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(OrderService.OrderNotFoundException.class)
    public ResponseEntity<OrderDtos.ApiError> handleNotFound(OrderService.OrderNotFoundException e,
                                                             HttpServletRequest request) {
        return build(HttpStatus.NOT_FOUND, "ORDER_NOT_FOUND", e.getMessage(), request, null);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<OrderDtos.ApiError> handleValidation(MethodArgumentNotValidException e,
                                                               HttpServletRequest request) {
        List<OrderDtos.FieldViolation> violations = e.getBindingResult().getFieldErrors().stream()
                .map(fe -> new OrderDtos.FieldViolation(fe.getField(), fe.getDefaultMessage()))
                .toList();
        return build(HttpStatus.BAD_REQUEST, "VALIDATION_FAILED",
                "Request body failed validation", request, violations);
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<OrderDtos.ApiError> handleConstraint(ConstraintViolationException e,
                                                               HttpServletRequest request) {
        List<OrderDtos.FieldViolation> violations = e.getConstraintViolations().stream()
                .map(v -> new OrderDtos.FieldViolation(v.getPropertyPath().toString(), v.getMessage()))
                .toList();
        return build(HttpStatus.BAD_REQUEST, "VALIDATION_FAILED",
                "Request parameters failed validation", request, violations);
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, IllegalArgumentException.class})
    public ResponseEntity<OrderDtos.ApiError> handleMalformed(Exception e, HttpServletRequest request) {
        // IllegalArgumentException lands here because the OrderLine record
        // validates its own invariants in a compact constructor; a bad quantity
        // is a client error, not a server fault.
        return build(HttpStatus.BAD_REQUEST, "MALFORMED_REQUEST",
                "Request could not be processed: " + e.getMessage(), request, null);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<OrderDtos.ApiError> handleUnexpected(Exception e, HttpServletRequest request) {
        log.error("Unhandled error on {} {}", request.getMethod(), request.getRequestURI(), e);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR",
                "Something went wrong on our side", request, null);
    }

    private ResponseEntity<OrderDtos.ApiError> build(HttpStatus status, String code, String message,
                                                     HttpServletRequest request,
                                                     List<OrderDtos.FieldViolation> violations) {
        return ResponseEntity.status(status).body(new OrderDtos.ApiError(
                Instant.now(), status.value(), code, message, request.getRequestURI(), violations));
    }
}
