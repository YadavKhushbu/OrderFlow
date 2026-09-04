package com.orderflow.order.repo;

import com.orderflow.order.domain.Order;
import com.orderflow.order.domain.OrderStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface OrderRepository extends JpaRepository<Order, Long> {

    @EntityGraph(attributePaths = {"lines"})
    Optional<Order> findWithLinesById(Long id);

    /** Saga replies arrive keyed by saga id, not order id. */
    @EntityGraph(attributePaths = {"lines"})
    Optional<Order> findWithLinesBySagaId(UUID sagaId);

    @EntityGraph(attributePaths = {"lines"})
    @Query("SELECT o FROM Order o WHERE o.customerRef = :customerRef ORDER BY o.id DESC")
    Page<Order> findByCustomer(@Param("customerRef") String customerRef, Pageable pageable);

    long countByStatus(OrderStatus status);
}
