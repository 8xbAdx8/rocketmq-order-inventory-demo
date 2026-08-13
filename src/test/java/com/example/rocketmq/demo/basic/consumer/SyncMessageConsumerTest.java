package com.example.rocketmq.demo.basic.consumer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Consumer 的纯单元测试，不启动 Spring，也不连接 RocketMQ。
 */
class SyncMessageConsumerTest {

    private final SyncMessageConsumer consumer = new SyncMessageConsumer();

    /**
     * 正常消息处理完成后应当正常返回，对应 RocketMQ 的消费成功。
     */
    @Test
    void shouldReturnNormallyForValidMessage() {
        assertDoesNotThrow(() -> consumer.onMessage("valid-message"));
    }

    /**
     * 非法消息必须将异常传播给监听容器，不能记日志后假装消费成功。
     */
    @Test
    void shouldPropagateExceptionForBlankMessage() {
        assertThrows(IllegalArgumentException.class, () -> consumer.onMessage(" "));
    }
}
