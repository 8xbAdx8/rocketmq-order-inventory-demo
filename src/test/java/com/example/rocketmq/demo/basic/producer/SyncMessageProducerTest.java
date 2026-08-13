package com.example.rocketmq.demo.basic.producer;

import com.example.rocketmq.infrastructure.messaging.RocketMqSendException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** 验证同步发送不会把非 SEND_OK 回执返回给业务层。 */
class SyncMessageProducerTest {

    @Test
    void shouldThrowWhenBrokerDoesNotConfirmSendOk() {
        RocketMQTemplate template = mock(RocketMQTemplate.class);
        SyncMessageProducer producer = new SyncMessageProducer(template, "topic", "tag");
        SendResult uncertainResult = mock(SendResult.class);
        when(uncertainResult.getSendStatus()).thenReturn(SendStatus.FLUSH_SLAVE_TIMEOUT);
        when(template.syncSend("topic:tag", "reliable-message")).thenReturn(uncertainResult);

        assertThrows(
                RocketMqSendException.class,
                () -> producer.send("reliable-message")
        );
    }
}
