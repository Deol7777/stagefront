package com.ticketing.payment.domain;

import org.springframework.data.jpa.repository.JpaRepository;

/** CRUD for payments (keyed by paymentId). */
public interface PaymentRepository extends JpaRepository<PaymentEntity, String> {
}
