package com.example.rocketmq.order.message;

import com.example.rocketmq.infrastructure.messaging.RocketMqSendResultValidator;
import com.example.rocketmq.infrastructure.messaging.MessageLogContext;
import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.service.port.OrderEventPublisher;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.UUID;

/**
 * 订单事件生产者。
 *
 * <p>该类是订单业务与 RocketMQ 之间的适配层：它负责将订单领域对象转换为稳定的
 * 消息契约，并处理 destination、Message Key 和 Broker 发送结果。</p>
 */
@Service
public class OrderMessageProducer implements OrderEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderMessageProducer.class);

    private final RocketMQTemplate rocketMQTemplate;
    private final String orderCreatedDestination;
    private final String orderTimeoutDestination;
    private final long orderTimeoutDelaySeconds;

    public OrderMessageProducer(
            RocketMQTemplate rocketMQTemplate,
            @Value("${demo.rocketmq.order-topic}") String topic,
            @Value("${demo.rocketmq.order-created-tag}") String orderCreatedTag,
            @Value("${demo.rocketmq.order-timeout-tag}") String orderTimeoutTag,
            @Value("${demo.rocketmq.order-timeout-delay-seconds}") long orderTimeoutDelaySeconds) {
        this.rocketMQTemplate = rocketMQTemplate;
        if (orderTimeoutDelaySeconds <= 0) {
            throw new IllegalArgumentException("订单超时延迟秒数必须大于 0");
        }
        this.orderCreatedDestination = topic + ":" + orderCreatedTag;
        this.orderTimeoutDestination = topic + ":" + orderTimeoutTag;
        this.orderTimeoutDelaySeconds = orderTimeoutDelaySeconds;
    }

    /**
     * 同步发送“订单已创建”事件。
     *
     * <p>同步是指当前调用线程等待 Broker 回执，并不代表库存同步扣减。库存业务仍会
     * 由 Consumer 异步执行。</p>
     *
     * @param order 已经成功写入数据库、带有数据库主键的订单
     * @return Broker 的发送结果
     */
    public SendResult sendOrderCreated(PurchaseOrder order) {
        validatePersistedOrder(order);

        OrderCreatedMessage event = new OrderCreatedMessage(
                order.eventId(),
                order.orderNo(),
                order.productId(),
                order.quantity(),
                order.createdAt()
        );

        Message<OrderCreatedMessage> message = MessageBuilder
                .withPayload(event)
                // Message Key 用于按业务订单号查询消息，不参与 Consumer 的 Tag 过滤。
                .setHeader(RocketMQHeaders.KEYS, order.orderNo())
                .build();

        try (MessageLogContext ignored = MessageLogContext.forSend(
                "ORDER_CREATED",
                event.eventId(),
                event.orderNo())) {
            log.info("消息发送开始，destination={}", orderCreatedDestination);
            try {
                SendResult sendResult = RocketMqSendResultValidator.requireSendOk(
                        orderCreatedDestination,
                        rocketMQTemplate.syncSend(orderCreatedDestination, message)
                );
                log.info(
                        "消息发送成功，destination={}, msgId={}, status={}",
                        orderCreatedDestination,
                        sendResult.getMsgId(),
                        sendResult.getSendStatus()
                );
                return sendResult;
            } catch (RuntimeException exception) {
                log.error("消息发送失败，destination={}", orderCreatedDestination, exception);
                throw exception;
            }
        }
    }

    @Override
    public void publishOrderCreated(PurchaseOrder order) {
        sendOrderCreated(order);
    }

    /**
     * 发送订单超时检查延迟消息。
     *
     * <p>{@link RocketMQTemplate#syncSendDelayTimeSeconds(String, Message, long)} 会把延迟秒数
     * 写入 RocketMQ 消息属性。Broker 接收后先进入定时存储，到期才让该消息对 Consumer
     * 可见，因此应用不需要为每个订单创建一个本地定时线程。</p>
     */
    public SendResult sendOrderTimeoutCheck(PurchaseOrder order) {
        validatePersistedOrder(order);

        OrderTimeoutMessage event = new OrderTimeoutMessage(
                UUID.randomUUID().toString(),
                order.orderNo(),
                order.createdAt()
        );

        Message<OrderTimeoutMessage> message = MessageBuilder
                .withPayload(event)
                .setHeader(RocketMQHeaders.KEYS, order.orderNo())
                .build();

        try (MessageLogContext ignored = MessageLogContext.forSend(
                "ORDER_TIMEOUT_CHECK",
                event.eventId(),
                event.orderNo())) {
            log.info(
                    "延迟消息发送开始，destination={}, delaySeconds={}",
                    orderTimeoutDestination,
                    orderTimeoutDelaySeconds
            );
            try {
                SendResult sendResult = RocketMqSendResultValidator.requireSendOk(
                        orderTimeoutDestination,
                        rocketMQTemplate.syncSendDelayTimeSeconds(
                                orderTimeoutDestination,
                                message,
                                orderTimeoutDelaySeconds
                        )
                );
                log.info(
                        "延迟消息发送成功，destination={}, delaySeconds={}, msgId={}, status={}",
                        orderTimeoutDestination,
                        orderTimeoutDelaySeconds,
                        sendResult.getMsgId(),
                        sendResult.getSendStatus()
                );
                return sendResult;
            } catch (RuntimeException exception) {
                log.error("延迟消息发送失败，destination={}", orderTimeoutDestination, exception);
                throw exception;
            }
        }
    }

    @Override
    public void publishOrderTimeoutCheck(PurchaseOrder order) {
        sendOrderTimeoutCheck(order);
    }

    private void validatePersistedOrder(PurchaseOrder order) {
        Objects.requireNonNull(order, "order 不能为空");
        if (order.id() == null) {
            throw new IllegalArgumentException("只有已持久化的订单才能发送事件");
        }
    }

}
