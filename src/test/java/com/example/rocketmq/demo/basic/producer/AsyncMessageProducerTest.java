package com.example.rocketmq.demo.basic.producer;

import com.example.rocketmq.infrastructure.messaging.RocketMqSendException;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 验证异步回调中的不确定状态不会被错误包装成成功 Future。 */
class AsyncMessageProducerTest {

    @Test
    void shouldCompleteFutureExceptionallyForNonSendOkCallback() {
        RocketMQTemplate template = mock(RocketMQTemplate.class);
        AsyncMessageProducer producer = new AsyncMessageProducer(template, "topic", "tag");
        ArgumentCaptor<SendCallback> callbackCaptor = ArgumentCaptor.forClass(SendCallback.class);
        SendResult uncertainResult = mock(SendResult.class);
        when(uncertainResult.getSendStatus()).thenReturn(SendStatus.FLUSH_DISK_TIMEOUT);

        CompletableFuture<SendResult> future = producer.send("reliable-message");
        verify(template).asyncSend(anyString(), anyString(), callbackCaptor.capture());
        callbackCaptor.getValue().onSuccess(uncertainResult);

        CompletionException exception = assertThrows(CompletionException.class, future::join);
        assertInstanceOf(RocketMqSendException.class, exception.getCause());
    }
}
