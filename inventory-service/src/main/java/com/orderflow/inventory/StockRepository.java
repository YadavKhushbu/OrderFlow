package com.orderflow.inventory;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Collection;
import java.util.List;

public interface StockRepository extends JpaRepository<StockItem, String> {

    /**
     * Loads every SKU on the order in one query.
     *
     * <p>Ordered by primary key so that two orders touching an overlapping set of
     * SKUs always visit them in the same sequence. Even under optimistic
     * locking this keeps write conflicts predictable rather than order-dependent.
     */
    List<StockItem> findBySkuInOrderBySku(Collection<String> skus);
}
