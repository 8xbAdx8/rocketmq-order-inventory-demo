package com.example.rocketmq.infrastructure.messaging;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** 验证只有 Broker 明确返回 SEND_OK 才被认定为可靠发送成功。 */
class RocketMqSendResultValidatorTest {

    @Test
    void shouldReturnOriginalResultWhenBrokerReturnsSendOk() {
        SendResult result = resultWithStatus(SendStatus.SEND_OK);

        assertSame(
                result,
                RocketMqSendResultValidator.requireSendOk("topic:tag", result)
        );
    }

    @Test
    void shouldRejectNullResult() {
        RocketMqSendException exception = assertThrows(
                RocketMqSendException.class,
                () -> RocketMqSendResultValidator.requireSendOk("topic:tag", null)
        );

        assertEquals("topic:tag", exception.getDestination());
        assertNull(exception.getSendStatus());
    }

    @Test
    void shouldRejectUncertainBrokerResult() {
        SendResult result = resultWithStatus(SendStatus.FLUSH_DISK_TIMEOUT);

        RocketMqSendException exception = assertThrows(
                RocketMqSendException.class,
                () -> RocketMqSendResultValidator.requireSendOk("topic:tag", result)
        );

        assertEquals("topic:tag", exception.getDestination());
        assertEquals(SendStatus.FLUSH_DISK_TIMEOUT, exception.getSendStatus());
    }

    private SendResult resultWithStatus(SendStatus status) {
        SendResult result = mock(SendResult.class);
        when(result.getSendStatus()).thenReturn(status);
        return result;
    }
}
