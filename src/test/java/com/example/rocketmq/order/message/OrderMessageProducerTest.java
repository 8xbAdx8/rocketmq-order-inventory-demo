package com.example.rocketmq.order.message;

import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.messaging.Message;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Producer 的快速单元测试，不建立网络连接。
 */
class OrderMessageProducerTest {

    private RocketMQTemplate rocketMQTemplate;
    private OrderMessageProducer orderMessageProducer;

    @BeforeEach
    void setUp() {
        rocketMQTemplate = mock(RocketMQTemplate.class);
        orderMessageProducer = new OrderMessageProducer(
                rocketMQTemplate,
                "order-event-topic",
                "order-created",
                "order-timeout-check",
                1800
        );
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void shouldConvertPersistedOrderAndSendToExpectedDestination() {
        PurchaseOrder order = persistedOrder();
        SendResult expectedResult = mock(SendResult.class);
        when(expectedResult.getSendStatus()).thenReturn(SendStatus.SEND_OK);
        when(expectedResult.getMsgId()).thenReturn("msg-1001");
        when(rocketMQTemplate.syncSend(eq("order-event-topic:order-created"), any(Message.class)))
                .thenReturn(expectedResult);

        SendResult actualResult = orderMessageProducer.sendOrderCreated(order);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(rocketMQTemplate).syncSend(
                eq("order-event-topic:order-created"),
                messageCaptor.capture()
        );

        Message<?> sentMessage = messageCaptor.getValue();
        OrderCreatedMessage payload = (OrderCreatedMessage) sentMessage.getPayload();
        assertSame(expectedResult, actualResult);
        assertEquals(order.eventId(), payload.eventId());
        assertEquals(order.orderNo(), payload.orderNo());
        assertEquals(order.productId(), payload.productId());
        assertEquals(order.quantity(), payload.quantity());
        assertEquals(order.createdAt(), payload.occurredAt());
        assertEquals(order.orderNo(), sentMessage.getHeaders().get(RocketMQHeaders.KEYS));
    }

    @Test
    void shouldRejectOrderThatHasNotBeenPersisted() {
        PurchaseOrder unsavedOrder = new PurchaseOrder(
                null,
                "ORD-1002",
                "event-1002",
                1001L,
                1,
                OrderStatus.CREATED,
                Instant.parse("2026-08-09T10:00:00Z"),
                Instant.parse("2026-08-09T10:00:00Z")
        );

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> orderMessageProducer.sendOrderCreated(unsavedOrder)
        );

        assertEquals("只有已持久化的订单才能发送事件", exception.getMessage());
        verifyNoInteractions(rocketMQTemplate);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void shouldSendTimeoutCheckWithConfiguredDelaySeconds() {
        PurchaseOrder order = persistedOrder();
        SendResult expectedResult = mock(SendResult.class);
        when(expectedResult.getSendStatus()).thenReturn(SendStatus.SEND_OK);
        when(expectedResult.getMsgId()).thenReturn("timeout-msg-1001");
        when(rocketMQTemplate.syncSendDelayTimeSeconds(
                eq("order-event-topic:order-timeout-check"),
                any(Message.class),
                eq(1800L)
        )).thenReturn(expectedResult);

        SendResult actualResult = orderMessageProducer.sendOrderTimeoutCheck(order);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(rocketMQTemplate).syncSendDelayTimeSeconds(
                eq("order-event-topic:order-timeout-check"),
                messageCaptor.capture(),
                eq(1800L)
        );

        Message<?> sentMessage = messageCaptor.getValue();
        OrderTimeoutMessage payload = (OrderTimeoutMessage) sentMessage.getPayload();
        assertSame(expectedResult, actualResult);
        assertEquals(order.orderNo(), payload.orderNo());
        assertEquals(order.createdAt(), payload.orderCreatedAt());
        assertTrue(!payload.eventId().isBlank());
        assertEquals(order.orderNo(), sentMessage.getHeaders().get(RocketMQHeaders.KEYS));
    }

    private PurchaseOrder persistedOrder() {
        Instant createdAt = Instant.parse("2026-08-09T10:00:00Z");
        return new PurchaseOrder(
                1L,
                "ORD-1001",
                "event-1001",
                1001L,
                2,
                OrderStatus.CREATED,
                createdAt,
                createdAt
        );
    }
}
