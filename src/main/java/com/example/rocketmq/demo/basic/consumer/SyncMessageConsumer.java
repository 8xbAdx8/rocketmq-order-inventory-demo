package com.example.rocketmq.demo.basic.consumer;

import com.example.rocketmq.infrastructure.messaging.RocketMqReliabilityPolicy;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * 同步教学消息的 Consumer。
 *
 * <p>RocketMQ 收到消息后，由 Starter 管理的监听容器调用
 * {@link #onMessage(String)}。这个类只演示消费边界和异常语义，
 * 真实库存扣减会在第三阶段放入独立业务 Service。</p>
 */
@Component
@RocketMQMessageListener(
        // 注解属性支持 Spring 占位符，避免把 Topic 和组名写死在 Java 代码中。
        topic = "${demo.rocketmq.topic}",
        selectorExpression = "${demo.rocketmq.sync-tag}",
        consumerGroup = "${demo.rocketmq.sync-consumer-group}",

        // CONCURRENTLY 表示普通并发消费；它不保证多条消息的业务顺序。
        consumeMode = ConsumeMode.CONCURRENTLY,

        // CLUSTERING 表示同组实例共同分担消息，这是企业业务消费的常见模式。
        messageModel = MessageModel.CLUSTERING,

        // 本地教学环境限制并发线程，便于观察日志和资源占用。
        consumeThreadNumber = 2,
        consumeThreadMax = 2,

        // 并发消费失败后最多重新消费 3 次；超过后进入死信队列。
        maxReconsumeTimes = RocketMqReliabilityPolicy.MAX_RECONSUME_TIMES)
public class SyncMessageConsumer implements RocketMQListener<String> {

    private static final Logger log = LoggerFactory.getLogger(SyncMessageConsumer.class);

    /**
     * 消费一条字符串消息。
     *
     * <p>方法正常返回表示消费成功；抛出异常表示消费失败。
     * 异常必须继续抛出，否则 RocketMQ 会把“打印了错误但吞掉异常”误判为消费成功。</p>
     *
     * @param messageBody Starter 反序列化后的消息正文
     */
    @Override
    public void onMessage(String messageBody) {
        try {
            if (!StringUtils.hasText(messageBody)) {
                throw new IllegalArgumentException("messageBody must not be blank");
            }

            // 当前阶段的“业务处理”只是记录收到的消息。
            // 第三阶段会替换为库存 Service 的幂等扣减操作。
            log.info("RocketMQ message consumed successfully: messageBody={}", messageBody);
        } catch (RuntimeException exception) {
            log.error("RocketMQ message consumption failed: messageBody={}", messageBody, exception);

            // 不吞异常：让监听容器把本次消费标记为失败，触发 RocketMQ 重试。
            throw exception;
        }
    }
}
