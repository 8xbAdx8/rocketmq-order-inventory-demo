package com.example.rocketmq.order.message;

import com.example.rocketmq.infrastructure.messaging.RocketMqSendResultValidator;
import com.example.rocketmq.infrastructure.messaging.MessageLogContext;
import com.example.rocketmq.order.service.port.OrderLifecyclePublisher;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

/**
 * 发送订单生命周期顺序消息。
 */
@Service
public class OrderLifecycleMessageProducer implements OrderLifecyclePublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderLifecycleMessageProducer.class);

    private final RocketMQTemplate rocketMQTemplate;
    private final String destination;

    public OrderLifecycleMessageProducer(
            RocketMQTemplate rocketMQTemplate,
            @Value("${demo.rocketmq.order-topic}") String topic,
            @Value("${demo.rocketmq.order-lifecycle-tag}") String tag) {
        this.rocketMQTemplate = rocketMQTemplate;
        this.destination = topic + ":" + tag;
    }

    /**
     * 发送一个生命周期事件。
     *
     * <p>第三个参数使用 {@code orderNo} 作为 hashKey。RocketMQTemplate 会基于该键选择
     * Message Queue，因此同一订单的 CREATED、PAID、SHIPPED 会稳定进入同一个队列。</p>
     */
    public SendResult send(String orderNo, OrderLifecycleEventType eventType) {
        OrderLifecycleMessage event = new OrderLifecycleMessage(
                UUID.randomUUID().toString(),
                orderNo,
                eventType,
                Instant.now()
        );
        return send(event);
    }

    @Override
    public void publish(String orderNo, OrderLifecycleEventType eventType) {
        send(orderNo, eventType);
    }

    /**
     * 发送已经构造好的消息，便于测试精确控制事件内容。
     */
    public SendResult send(OrderLifecycleMessage event) {
        Message<OrderLifecycleMessage> message = MessageBuilder
                .withPayload(event)
                .setHeader(RocketMQHeaders.KEYS, event.orderNo())
                .build();

        try (MessageLogContext ignored = MessageLogContext.forSend(
                "ORDER_LIFECYCLE_" + event.eventType().name(),
                event.eventId(),
                event.orderNo())) {
            log.info("顺序消息发送开始，destination={}, hashKey={}", destination, event.orderNo());
            try {
                SendResult sendResult = RocketMqSendResultValidator.requireSendOk(
                        destination,
                        rocketMQTemplate.syncSendOrderly(destination, message, event.orderNo())
                );
                log.info(
                        "顺序消息发送成功，destination={}, hashKey={}, msgId={}, status={}, queue={}",
                        destination,
                        event.orderNo(),
                        sendResult.getMsgId(),
                        sendResult.getSendStatus(),
                        sendResult.getMessageQueue()
                );
                return sendResult;
            } catch (RuntimeException exception) {
                log.error("顺序消息发送失败，destination={}, hashKey={}", destination, event.orderNo(), exception);
                throw exception;
            }
        }
    }
}
