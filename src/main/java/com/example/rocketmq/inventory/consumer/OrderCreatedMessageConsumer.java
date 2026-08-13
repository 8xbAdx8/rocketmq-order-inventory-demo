package com.example.rocketmq.inventory.consumer;

import com.example.rocketmq.infrastructure.messaging.RocketMqReliabilityPolicy;
import com.example.rocketmq.infrastructure.messaging.MessageLogContext;
import com.example.rocketmq.inventory.exception.InsufficientStockException;
import com.example.rocketmq.inventory.exception.InventoryNotFoundException;
import com.example.rocketmq.inventory.service.InventoryMessageConsumeService;
import com.example.rocketmq.inventory.service.MessageConsumeResult;
import com.example.rocketmq.order.message.OrderCreatedMessage;
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
 * 库存模块的“订单已创建”消息消费者。
 *
 * <p>监听器只承担消息边界职责：接收并反序列化事件、记录消息上下文、调用库存业务
 * Service，以及把失败正确反馈给 RocketMQ。原子扣减 SQL 不应写在监听器中。</p>
 */
@Component
@RocketMQMessageListener(
        topic = "${demo.rocketmq.order-topic}",
        consumerGroup = "${demo.rocketmq.inventory-consumer-group}",

        // TAG 过滤发生在 Broker/Consumer 拉取链路，只接收“订单已创建”事件。
        selectorType = SelectorType.TAG,
        selectorExpression = "${demo.rocketmq.order-created-tag}",

        // 普通订单之间没有顺序要求，可以由多个线程并发处理。
        consumeMode = ConsumeMode.CONCURRENTLY,

        // 同一个 Group 内多个库存实例共同分担消息，每条消息只分配给其中一个实例。
        messageModel = MessageModel.CLUSTERING,
        consumeThreadNumber = 2,
        consumeThreadMax = 2,

        // 失败后最多重新消费 3 次；仍失败则进入该 Consumer Group 的死信队列。
        maxReconsumeTimes = RocketMqReliabilityPolicy.MAX_RECONSUME_TIMES)
public class OrderCreatedMessageConsumer implements RocketMQListener<OrderCreatedMessage> {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedMessageConsumer.class);

    private final InventoryMessageConsumeService messageConsumeService;
    private final String consumerGroup;

    public OrderCreatedMessageConsumer(
            InventoryMessageConsumeService messageConsumeService,
            @Value("${demo.rocketmq.inventory-consumer-group}") String consumerGroup) {
        this.messageConsumeService = messageConsumeService;
        this.consumerGroup = consumerGroup;
    }

    /**
     * 消费订单创建事件并扣减库存。
     *
     * <p>方法正常返回代表 CONSUME_SUCCESS；抛出异常代表本次消费失败，监听容器会把
     * 消息交给 RocketMQ 重试机制。任何异常都不能只记录后吞掉。</p>
     */
    @Override
    public void onMessage(OrderCreatedMessage message) {
        Objects.requireNonNull(message, "订单创建消息不能为空");

        try (MessageLogContext ignored = MessageLogContext.forConsume(
                "ORDER_CREATED",
                message.eventId(),
                message.orderNo(),
                consumerGroup)) {
            log.info(
                    "消息消费开始，productId={}, quantity={}",
                    message.productId(),
                    message.quantity()
            );
            try {
                MessageConsumeResult result = messageConsumeService.consumeOrderCreated(
                        consumerGroup,
                        message
                );
                log.info("消息消费成功，result={}", result);
            } catch (InsufficientStockException | InventoryNotFoundException businessException) {
                /*
                 * 这是明确的业务失败。当前阶段尚未实现“库存扣减失败”补偿事件，
                 * 因此不能返回成功并丢掉消息，先让它进入重试，最终由 DLQ 保留待处理记录。
                 */
                log.warn("消息消费遇到业务失败，将触发重试，reason={}", businessException.getMessage());
                throw businessException;
            } catch (RuntimeException technicalException) {
                // 数据库超时、连接中断、死锁等技术异常通常具有可恢复性，应交给 RocketMQ 重试。
                log.error("消息消费发生技术异常，将触发重试", technicalException);
                throw technicalException;
            }
        }
    }
}
