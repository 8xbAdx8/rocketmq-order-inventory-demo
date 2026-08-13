package com.example.rocketmq.order.consumer;

import com.example.rocketmq.infrastructure.messaging.MessageLogContext;
import com.example.rocketmq.infrastructure.messaging.RocketMqReliabilityPolicy;
import com.example.rocketmq.order.message.OrderLifecycleMessage;
import com.example.rocketmq.order.service.OrderLifecycleEventService;
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
 * 订单生命周期顺序消息消费者。
 */
@Component
@RocketMQMessageListener(
        topic = "${demo.rocketmq.order-topic}",
        consumerGroup = "${demo.rocketmq.order-lifecycle-consumer-group}",
        selectorType = SelectorType.TAG,
        selectorExpression = "${demo.rocketmq.order-lifecycle-tag}",

        // ORDERLY 会对分配到当前实例的 Message Queue 串行消费。
        consumeMode = ConsumeMode.ORDERLY,
        messageModel = MessageModel.CLUSTERING,
        consumeThreadNumber = 2,
        consumeThreadMax = 2,
        maxReconsumeTimes = RocketMqReliabilityPolicy.MAX_RECONSUME_TIMES)
public class OrderLifecycleMessageConsumer implements RocketMQListener<OrderLifecycleMessage> {

    private static final Logger log = LoggerFactory.getLogger(OrderLifecycleMessageConsumer.class);

    private final OrderLifecycleEventService lifecycleEventService;
    private final String consumerGroup;

    public OrderLifecycleMessageConsumer(
            OrderLifecycleEventService lifecycleEventService,
            @Value("${demo.rocketmq.order-lifecycle-consumer-group}") String consumerGroup) {
        this.lifecycleEventService = lifecycleEventService;
        this.consumerGroup = consumerGroup;
    }

    @Override
    public void onMessage(OrderLifecycleMessage message) {
        Objects.requireNonNull(message, "订单生命周期消息不能为空");
        try (MessageLogContext ignored = MessageLogContext.forConsume(
                "ORDER_LIFECYCLE_" + message.eventType().name(),
                message.eventId(),
                message.orderNo(),
                consumerGroup)) {
            log.info("顺序消息消费开始，eventType={}", message.eventType());
            try {
                lifecycleEventService.recordInOrder(message);
                log.info("顺序消息消费成功，eventType={}", message.eventType());
            } catch (RuntimeException exception) {
                // 顺序消费失败时必须抛出；RocketMQ 会暂停该队列并重试，避免后续消息越过失败消息。
                log.error("顺序消息消费失败，将保持队列顺序并重试，eventType={}", message.eventType(), exception);
                throw exception;
            }
        }
    }
}
