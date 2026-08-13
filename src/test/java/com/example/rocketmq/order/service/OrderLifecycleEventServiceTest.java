package com.example.rocketmq.order.service;

import com.example.rocketmq.order.message.OrderLifecycleEventType;
import com.example.rocketmq.order.message.OrderLifecycleMessage;
import com.example.rocketmq.order.repository.OrderLifecycleEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderLifecycleEventServiceTest {

    private OrderLifecycleEventRepository repository;
    private OrderLifecycleEventService service;

    @BeforeEach
    void setUp() {
        repository = mock(OrderLifecycleEventRepository.class);
        service = new OrderLifecycleEventService(repository);
    }

    @Test
    void shouldAcceptPaidAfterCreated() {
        OrderLifecycleMessage message = message(OrderLifecycleEventType.PAID);
        when(repository.findLatestEventType("ORD-1001"))
                .thenReturn(Optional.of(OrderLifecycleEventType.CREATED));

        service.recordInOrder(message);

        verify(repository).append(message);
    }

    @Test
    void shouldRejectShippedBeforePaid() {
        OrderLifecycleMessage message = message(OrderLifecycleEventType.SHIPPED);
        when(repository.findLatestEventType("ORD-1001"))
                .thenReturn(Optional.of(OrderLifecycleEventType.CREATED));

        assertThrows(
                IllegalStateException.class,
                () -> service.recordInOrder(message)
        );

        verify(repository, never()).append(message);
    }

    private OrderLifecycleMessage message(OrderLifecycleEventType eventType) {
        return new OrderLifecycleMessage(
                "event-" + eventType.name(),
                "ORD-1001",
                eventType,
                Instant.parse("2026-08-10T02:00:00Z")
        );
    }
}
