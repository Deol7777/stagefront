package com.ticketing.payment.web;

import com.ticketing.payment.domain.PaymentEntity;
import com.ticketing.payment.domain.PaymentRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Read-only payment listing for the debug dashboard. */
@RestController
@RequestMapping("/api/payments")
public class PaymentController {

    private final PaymentRepository payments;

    public PaymentController(PaymentRepository payments) {
        this.payments = payments;
    }

    @GetMapping
    public List<PaymentEntity> list() {
        return payments.findAll();
    }
}
