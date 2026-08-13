package com.example.rocketmq.inventory.consumer;

import com.example.rocketmq.inventory.exception.InsufficientStockException;
import com.example.rocketmq.inventory.exception.InventoryNotFoundException;
import com.example.rocketmq.inventory.service.InventoryMessageConsumeService;
import com.example.rocketmq.inventory.service.MessageConsumeResult;
import com.example.rocketmq.order.message.OrderCreatedMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Consumer 的快速单元测试，不启动 Spring、MySQL 或 RocketMQ。
 */
class OrderCreatedMessageConsumerTest {

    private InventoryMessageConsumeService messageConsumeService;
    private OrderCreatedMessageConsumer consumer;

    @BeforeEach
    void setUp() {
        messageConsumeService = mock(InventoryMessageConsumeService.class);
        consumer = new OrderCreatedMessageConsumer(messageConsumeService, "inventory-test-group");
    }

    @Test
    void shouldDelegateMessageToIdempotentConsumeService() {
        OrderCreatedMessage message = message();
        org.mockito.Mockito.when(messageConsumeService.consumeOrderCreated(
                "inventory-test-group",
                message
        )).thenReturn(MessageConsumeResult.PROCESSED);

        consumer.onMessage(message);

        verify(messageConsumeService).consumeOrderCreated("inventory-test-group", message);
    }

    @Test
    void shouldRethrowInsufficientStockForRocketMqRetry() {
        OrderCreatedMessage message = message();
        InsufficientStockException expected =
                new InsufficientStockException(1001L, 2, 1);
        doThrow(expected)
                .when(messageConsumeService)
                .consumeOrderCreated("inventory-test-group", message);

        InsufficientStockException actual = assertThrows(
                InsufficientStockException.class,
                () -> consumer.onMessage(message)
        );

        assertSame(expected, actual);
    }

    @Test
    void shouldRethrowInventoryNotFoundForRocketMqRetry() {
        OrderCreatedMessage message = message();
        InventoryNotFoundException expected = new InventoryNotFoundException(1001L);
        doThrow(expected)
                .when(messageConsumeService)
                .consumeOrderCreated("inventory-test-group", message);

        InventoryNotFoundException actual = assertThrows(
                InventoryNotFoundException.class,
                () -> consumer.onMessage(message)
        );

        assertSame(expected, actual);
    }

    @Test
    void shouldRethrowTechnicalExceptionForRocketMqRetry() {
        OrderCreatedMessage message = message();
        IllegalStateException expected = new IllegalStateException("模拟数据库连接中断");
        doThrow(expected)
                .when(messageConsumeService)
                .consumeOrderCreated("inventory-test-group", message);

        IllegalStateException actual = assertThrows(
                IllegalStateException.class,
                () -> consumer.onMessage(message)
        );

        assertSame(expected, actual);
    }

    private OrderCreatedMessage message() {
        return new OrderCreatedMessage(
                "event-1001",
                "ORD-1001",
                1001L,
                2,
                Instant.parse("2026-08-09T10:00:00Z")
        );
    }
}
