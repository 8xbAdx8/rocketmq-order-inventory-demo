package com.example.rocketmq.order.message;

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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderLifecycleMessageProducerTest {

    private RocketMQTemplate rocketMQTemplate;
    private OrderLifecycleMessageProducer producer;

    @BeforeEach
    void setUp() {
        rocketMQTemplate = mock(RocketMQTemplate.class);
        producer = new OrderLifecycleMessageProducer(
                rocketMQTemplate,
                "order-event-topic",
                "order-lifecycle"
        );
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void shouldUseOrderNoAsOrderlyQueueHashKey() {
        OrderLifecycleMessage event = new OrderLifecycleMessage(
                "event-1001",
                "ORD-1001",
                OrderLifecycleEventType.PAID,
                Instant.parse("2026-08-10T02:00:00Z")
        );
        SendResult expectedResult = mock(SendResult.class);
        when(expectedResult.getSendStatus()).thenReturn(SendStatus.SEND_OK);
        when(expectedResult.getMsgId()).thenReturn("msg-1001");
        when(rocketMQTemplate.syncSendOrderly(
                eq("order-event-topic:order-lifecycle"),
                any(Message.class),
                eq("ORD-1001")
        )).thenReturn(expectedResult);

        SendResult actualResult = producer.send(event);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(rocketMQTemplate).syncSendOrderly(
                eq("order-event-topic:order-lifecycle"),
                messageCaptor.capture(),
                eq("ORD-1001")
        );
        assertSame(expectedResult, actualResult);
        assertSame(event, messageCaptor.getValue().getPayload());
        assertEquals(
                "ORD-1001",
                messageCaptor.getValue().getHeaders().get(RocketMQHeaders.KEYS)
        );
    }
}
