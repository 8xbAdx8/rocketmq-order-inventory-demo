package com.example.rocketmq.order.consumer;

import com.example.rocketmq.order.message.OrderTimeoutMessage;
import com.example.rocketmq.order.service.OrderTimeoutService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OrderTimeoutMessageConsumerTest {

    private OrderTimeoutService orderTimeoutService;
    private OrderTimeoutMessageConsumer consumer;

    @BeforeEach
    void setUp() {
        orderTimeoutService = mock(OrderTimeoutService.class);
        consumer = new OrderTimeoutMessageConsumer(orderTimeoutService, "timeout-test-group");
    }

    @Test
    void shouldDelegateTimeoutCheckToService() {
        OrderTimeoutMessage message = message();

        consumer.onMessage(message);

        verify(orderTimeoutService).cancelIfUnpaid(message.orderNo());
    }

    @Test
    void shouldRethrowTechnicalFailureForRocketMqRetry() {
        OrderTimeoutMessage message = message();
        IllegalStateException expected = new IllegalStateException("模拟数据库连接中断");
        doThrow(expected).when(orderTimeoutService).cancelIfUnpaid(message.orderNo());

        IllegalStateException actual = assertThrows(
                IllegalStateException.class,
                () -> consumer.onMessage(message)
        );

        assertSame(expected, actual);
    }

    private OrderTimeoutMessage message() {
        return new OrderTimeoutMessage(
                "timeout-event-1001",
                "ORD-1001",
                Instant.parse("2026-08-10T01:00:00Z")
        );
    }
}
