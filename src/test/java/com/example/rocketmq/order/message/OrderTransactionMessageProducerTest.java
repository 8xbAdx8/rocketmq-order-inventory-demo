package com.example.rocketmq.order.message;

import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.transaction.CreateOrderTransactionContext;
import com.example.rocketmq.order.transaction.OrderTransactionHeaders;
import com.example.rocketmq.order.service.port.OrderTransactionOutcome;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.messaging.Message;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 不连接 Broker，验证事务 Producer 对 Starter 高层 API 的使用。 */
class OrderTransactionMessageProducerTest {

    private RocketMQTemplate rocketMQTemplate;
    private OrderTransactionMessageProducer producer;

    @BeforeEach
    void setUp() {
        rocketMQTemplate = mock(RocketMQTemplate.class);
        producer = new OrderTransactionMessageProducer(
                rocketMQTemplate,
                "order-event-topic",
                "order-created"
        );
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void shouldSendHalfMessageWithBusinessHeadersAndLocalContext() {
        PurchaseOrder order = unsavedOrder();
        TransactionSendResult expectedResult = mock(TransactionSendResult.class);
        when(expectedResult.getSendStatus()).thenReturn(SendStatus.SEND_OK);
        when(expectedResult.getLocalTransactionState()).thenReturn(LocalTransactionState.COMMIT_MESSAGE);
        when(expectedResult.getMsgId()).thenReturn("transaction-msg-1001");
        when(rocketMQTemplate.sendMessageInTransaction(
                eq("order-event-topic:order-created"),
                any(Message.class),
                any(CreateOrderTransactionContext.class)
        )).thenReturn(expectedResult);

        OrderTransactionOutcome actual = producer.sendCreateOrderTransaction(order);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        ArgumentCaptor<CreateOrderTransactionContext> contextCaptor =
                ArgumentCaptor.forClass(CreateOrderTransactionContext.class);
        verify(rocketMQTemplate).sendMessageInTransaction(
                eq("order-event-topic:order-created"),
                messageCaptor.capture(),
                contextCaptor.capture()
        );

        Message<?> sentMessage = messageCaptor.getValue();
        OrderCreatedMessage payload = (OrderCreatedMessage) sentMessage.getPayload();
        assertEquals("transaction-msg-1001", actual.messageId());
        assertEquals(OrderTransactionOutcome.LocalState.COMMITTED, actual.localState());
        assertSame(order, contextCaptor.getValue().order());
        assertEquals(order.orderNo(), payload.orderNo());
        assertEquals(order.eventId(), payload.eventId());
        assertEquals(order.orderNo(), sentMessage.getHeaders().get(RocketMQHeaders.KEYS));
        assertEquals(order.orderNo(), sentMessage.getHeaders().get(OrderTransactionHeaders.ORDER_NO));
        assertEquals(order.eventId(), sentMessage.getHeaders().get(OrderTransactionHeaders.EVENT_ID));
    }

    private PurchaseOrder unsavedOrder() {
        Instant now = Instant.parse("2026-08-10T01:00:00Z");
        return new PurchaseOrder(
                null,
                "TX-ORD-1001",
                "event-1001",
                1001L,
                2,
                OrderStatus.CREATED,
                now,
                now
        );
    }
}
