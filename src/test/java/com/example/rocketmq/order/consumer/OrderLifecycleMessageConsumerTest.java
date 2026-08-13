package com.example.rocketmq.order.consumer;

import com.example.rocketmq.order.message.OrderLifecycleEventType;
import com.example.rocketmq.order.message.OrderLifecycleMessage;
import com.example.rocketmq.order.service.OrderLifecycleEventService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OrderLifecycleMessageConsumerTest {

    private OrderLifecycleEventService service;
    private OrderLifecycleMessageConsumer consumer;

    @BeforeEach
    void setUp() {
        service = mock(OrderLifecycleEventService.class);
        consumer = new OrderLifecycleMessageConsumer(service, "lifecycle-test-group");
    }

    @Test
    void shouldDelegateMessageToLifecycleService() {
        OrderLifecycleMessage message = message();

        consumer.onMessage(message);

        verify(service).recordInOrder(message);
    }

    @Test
    void shouldRethrowFailureToKeepQueueOrder() {
        OrderLifecycleMessage message = message();
        IllegalStateException expected = new IllegalStateException("模拟乱序");
        doThrow(expected).when(service).recordInOrder(message);

        IllegalStateException actual = assertThrows(
                IllegalStateException.class,
                () -> consumer.onMessage(message)
        );

        assertSame(expected, actual);
    }

    private OrderLifecycleMessage message() {
        return new OrderLifecycleMessage(
                "event-1001",
                "ORD-1001",
                OrderLifecycleEventType.CREATED,
                Instant.parse("2026-08-10T02:00:00Z")
        );
    }
}
