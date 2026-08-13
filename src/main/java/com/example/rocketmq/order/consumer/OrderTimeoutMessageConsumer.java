package com.example.rocketmq.order.consumer;

import com.example.rocketmq.infrastructure.messaging.MessageLogContext;
import com.example.rocketmq.infrastructure.messaging.RocketMqReliabilityPolicy;
import com.example.rocketmq.order.message.OrderTimeoutMessage;
import com.example.rocketmq.order.service.OrderTimeoutService;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * 消费到期的订单超时检查消息。
 */
@Component
@RocketMQMessageListener(
        topic = "${demo.rocketmq.order-topic}",
        consumerGroup = "${demo.rocketmq.order-timeout-consumer-group}",
        selectorType = SelectorType.TAG,
        selectorExpression = "${demo.rocketmq.order-timeout-tag}",
        consumeMode = ConsumeMode.CONCURRENTLY,
        messageModel = MessageModel.CLUSTERING,
        consumeThreadNumber = 2,
        consumeThreadMax = 2,
        maxReconsumeTimes = RocketMqReliabilityPolicy.MAX_RECONSUME_TIMES)
public class OrderTimeoutMessageConsumer implements RocketMQListener<OrderTimeoutMessage> {

    private static final Logger log = LoggerFactory.getLogger(OrderTimeoutMessageConsumer.class);

    private final OrderTimeoutService orderTimeoutService;
    private final String consumerGroup;

    public OrderTimeoutMessageConsumer(
            OrderTimeoutService orderTimeoutService,
            @Value("${demo.rocketmq.order-timeout-consumer-group}") String consumerGroup) {
        this.orderTimeoutService = orderTimeoutService;
        this.consumerGroup = consumerGroup;
    }

    @Override
    public void onMessage(OrderTimeoutMessage message) {
        Objects.requireNonNull(message, "订单超时消息不能为空");

        try (MessageLogContext ignored = MessageLogContext.forConsume(
                "ORDER_TIMEOUT_CHECK",
                message.eventId(),
                message.orderNo(),
                consumerGroup)) {
            log.info("消息消费开始，orderCreatedAt={}", message.orderCreatedAt());
            try {
                orderTimeoutService.cancelIfUnpaid(message.orderNo());
                log.info("消息消费成功");
            } catch (RuntimeException exception) {
                // 数据库连接、死锁等异常继续抛出，让 RocketMQ 按监听器配置重新投递。
                log.error("消息消费失败，将触发重试", exception);
                throw exception;
            }
        }
    }
}
