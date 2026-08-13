package com.example.rocketmq.demo.basic.producer;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 异步 Producer 的真实 RocketMQ 集成测试。
 *
 * <p>类名以 IT 结尾，普通的 {@code mvn test} 不会自动执行它，
 * 避免在未启动 RocketMQ 时影响日常单元测试。需要时在 IDEA 中手动运行。</p>
 */
@Tag("integration")
@SpringBootTest
class AsyncMessageProducerIT {

    @Autowired
    private AsyncMessageProducer asyncMessageProducer;

    @Test
    void shouldSendOrdinaryMessageAsynchronously() throws Exception {
        // 唯一 trace 便于在 Producer 与 Consumer 日志中确认是同一条消息。
        String messageBody = "Hello RocketMQ async, trace=" + UUID.randomUUID();

        CompletableFuture<SendResult> sendFuture = asyncMessageProducer.send(messageBody);

        /*
         * 只在测试中使用 get 等待回调，防止测试进程提前结束，并给出 10 秒超时保护。
         * 生产业务代码中不要在请求线程里立即 get，否则会抵消异步发送的意义。
         */
        SendResult sendResult = sendFuture.get(10, TimeUnit.SECONDS);

        assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
        assertNotNull(sendResult.getMsgId());

        System.out.printf(
                "异步消息发送成功：body=%s, status=%s, msgId=%s%n",
                messageBody,
                sendResult.getSendStatus(),
                sendResult.getMsgId()
        );
    }
}
