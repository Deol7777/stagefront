package com.ticketing.notification.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

/** CRUD for notifications. */
public interface NotificationRepository extends JpaRepository<NotificationEntity, UUID> {

    /** Notifications for one order (for the debug dashboard). */
    List<NotificationEntity> findByOrderId(String orderId);
}
