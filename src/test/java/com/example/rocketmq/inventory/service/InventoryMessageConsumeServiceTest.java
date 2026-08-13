package com.example.rocketmq.inventory.service;

import com.example.rocketmq.infrastructure.messaging.ConsumedMessageRepository;
import com.example.rocketmq.order.message.OrderCreatedMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 幂等消费编排的快速测试，不连接 MySQL 和 RocketMQ。 */
class InventoryMessageConsumeServiceTest {

    private static final String CONSUMER_GROUP = "inventory-test-group";

    private ConsumedMessageRepository consumedMessageRepository;
    private InventoryService inventoryService;
    private InventoryMessageConsumeService messageConsumeService;

    @BeforeEach
    void setUp() {
        consumedMessageRepository = mock(ConsumedMessageRepository.class);
        inventoryService = mock(InventoryService.class);
        messageConsumeService = new InventoryMessageConsumeService(
                consumedMessageRepository,
                inventoryService
        );
    }

    @Test
    void shouldDeductStockForFirstDelivery() {
        OrderCreatedMessage message = message("event-1001", "ORD-1001");
        when(consumedMessageRepository.tryRecord(
                eq(CONSUMER_GROUP),
                eq(message.eventId()),
                eq("ORDER_CREATED"),
                eq(message.orderNo()),
                any(Instant.class)
        )).thenReturn(true);

        MessageConsumeResult result = messageConsumeService.consumeOrderCreated(
                CONSUMER_GROUP,
                message
        );

        assertEquals(MessageConsumeResult.PROCESSED, result);
        verify(inventoryService).deductStock(message.productId(), message.quantity());
    }

    @Test
    void shouldSkipStockDeductionForDuplicateDelivery() {
        OrderCreatedMessage message = message("event-1001", "ORD-1001");
        when(consumedMessageRepository.tryRecord(
                eq(CONSUMER_GROUP),
                eq(message.eventId()),
                eq("ORDER_CREATED"),
                eq(message.orderNo()),
                any(Instant.class)
        )).thenReturn(false);
        when(consumedMessageRepository.findBusinessKey(CONSUMER_GROUP, message.eventId()))
                .thenReturn(Optional.of(message.orderNo()));

        MessageConsumeResult result = messageConsumeService.consumeOrderCreated(
                CONSUMER_GROUP,
                message
        );

        assertEquals(MessageConsumeResult.DUPLICATE_SKIPPED, result);
        verify(inventoryService, never()).deductStock(any(), any());
    }

    @Test
    void shouldRejectEventIdCollisionAcrossDifferentOrders() {
        OrderCreatedMessage message = message("event-1001", "ORD-NEW");
        when(consumedMessageRepository.tryRecord(
                eq(CONSUMER_GROUP),
                eq(message.eventId()),
                eq("ORDER_CREATED"),
                eq(message.orderNo()),
                any(Instant.class)
        )).thenReturn(false);
        when(consumedMessageRepository.findBusinessKey(CONSUMER_GROUP, message.eventId()))
                .thenReturn(Optional.of("ORD-OLD"));

        assertThrows(
                IllegalStateException.class,
                () -> messageConsumeService.consumeOrderCreated(CONSUMER_GROUP, message)
        );
        verify(inventoryService, never()).deductStock(any(), any());
    }

    private OrderCreatedMessage message(String eventId, String orderNo) {
        return new OrderCreatedMessage(
                eventId,
                orderNo,
                1001L,
                2,
                Instant.parse("2026-08-10T01:00:00Z")
        );
    }
}
