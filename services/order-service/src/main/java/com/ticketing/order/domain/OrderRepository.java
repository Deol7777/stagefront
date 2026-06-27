package com.ticketing.order.domain;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data repository for orders. Extending JpaRepository gives CRUD
 * (save, findById, findAll, ...) with no implementation to write.
 */
public interface OrderRepository extends JpaRepository<OrderEntity, String> {
}
