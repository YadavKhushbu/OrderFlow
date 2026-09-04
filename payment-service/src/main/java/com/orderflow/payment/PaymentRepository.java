package com.orderflow.payment;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface PaymentRepository extends JpaRepository<Payment, Long> {

    Optional<Payment> findBySagaId(UUID sagaId);

    Optional<Payment> findByIdempotencyKey(String idempotencyKey);
}
