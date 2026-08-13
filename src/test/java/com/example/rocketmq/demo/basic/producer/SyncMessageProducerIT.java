package com.example.rocketmq.demo.basic.producer;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 同步生产者集成测试。
 *
 * <p>类名以 IT（Integration Test）结尾，普通 {@code mvn test} 不会自动执行它。
 * 原因是它会向真实 Broker 写入消息，必须由开发者明确触发。</p>
 */
@Tag("integration")
@SpringBootTest
class SyncMessageProducerIT {

    @Autowired
    private SyncMessageProducer syncMessageProducer;

    /**
     * 发送一条带随机标识的消息，并验证 Broker 返回 SEND_OK 和 msgId。
     */
    @Test
    void shouldSendOrdinaryMessageSynchronously() {
        String messageBody = "Hello RocketMQ, trace=" + UUID.randomUUID();

        SendResult sendResult = syncMessageProducer.send(messageBody);

        assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
        assertNotNull(sendResult.getMsgId());

        // IDEA 测试窗口中可以直接查看完整发送结果。
        System.out.println("Sync message SendResult: " + sendResult);
    }
}
