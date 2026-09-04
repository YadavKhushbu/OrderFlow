package com.orderflow.messaging;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEvent, Long> {

    /**
     * The next batch to publish, oldest first.
     *
     * <p>Ordering by id matters: it is insertion order, so messages leave in the
     * order the business logic produced them. Combined with keying on the saga
     * id, that is what stops a compensation from arriving before the action it
     * is meant to undo.
     */
    @Query("SELECT e FROM OutboxEvent e WHERE e.publishedAt IS NULL ORDER BY e.id ASC")
    List<OutboxEvent> findPendingBatch(Pageable pageable);

    long countByPublishedAtIsNull();
}
