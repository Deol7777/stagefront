package com.ticketing.payment.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/** CRUD for payments (keyed by paymentId). */
public interface PaymentRepository extends JpaRepository<PaymentEntity, String> {

    /** The authorized payment for an order, if any (what a refund reverses). */
    Optional<PaymentEntity> findFirstByOrderIdAndStatus(String orderId, PaymentStatus status);
}
