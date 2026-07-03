package com.ticketing.notification.app;

import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.events.EventType;
import com.ticketing.contracts.events.OrderConfirmed;
import com.ticketing.notification.consumer.ProcessedEventRepository;
import com.ticketing.notification.domain.NotificationEntity;
import com.ticketing.notification.domain.NotificationRepository;
import com.ticketing.notification.domain.NotificationType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for notification handling. The key property: a duplicate event must
 * not create a second notification (no user spam).
 */
@ExtendWith(MockitoExtension.class)
class NotificationServiceTest {

    @Mock NotificationRepository notifications;
    @Mock ProcessedEventRepository processed;

    private NotificationService service() {
        return new NotificationService(notifications, processed);
    }

    private EventEnvelope<OrderConfirmed> confirmed() {
        return EventType.ORDER_CONFIRMED.newEnvelope(null,
                new OrderConfirmed("order-1", "user-1", "seat-1", "pay-1", Instant.now()));
    }

    @Test
    void createsNotificationOnConfirm() {
        when(processed.existsById("OrderConfirmed#order-1")).thenReturn(false);

        service().onOrderConfirmed(confirmed());

        var saved = ArgumentCaptor.forClass(NotificationEntity.class);
        verify(notifications).save(saved.capture());
        assertEquals(NotificationType.ORDER_CONFIRMED, saved.getValue().getType());
        assertEquals("order-1", saved.getValue().getOrderId());
        verify(processed).save(any());
    }

    @Test
    void doesNotNotifyTwiceForDuplicate() {
        when(processed.existsById("OrderConfirmed#order-1")).thenReturn(true);

        service().onOrderConfirmed(confirmed());

        verify(notifications, never()).save(any());
        verify(processed, never()).save(any());
    }
}
