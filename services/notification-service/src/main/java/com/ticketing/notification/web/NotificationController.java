package com.ticketing.notification.web;

import com.ticketing.notification.domain.NotificationEntity;
import com.ticketing.notification.domain.NotificationRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Read-only API for the debug dashboard: list notifications (optionally by order). */
@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

    private final NotificationRepository notifications;

    public NotificationController(NotificationRepository notifications) {
        this.notifications = notifications;
    }

    @GetMapping
    public List<NotificationEntity> list(@RequestParam(required = false) String orderId) {
        return orderId == null ? notifications.findAll() : notifications.findByOrderId(orderId);
    }
}
