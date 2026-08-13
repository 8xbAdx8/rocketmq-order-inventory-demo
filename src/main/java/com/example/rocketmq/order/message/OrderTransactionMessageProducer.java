package com.example.rocketmq.order.message;

import com.example.rocketmq.infrastructure.messaging.RocketMqSendResultValidator;
import com.example.rocketmq.infrastructure.messaging.MessageLogContext;
import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.transaction.CreateOrderTransactionContext;
import com.example.rocketmq.order.transaction.OrderTransactionHeaders;
import com.example.rocketmq.order.service.port.OrderTransactionOutcome;
import com.example.rocketmq.order.service.port.OrderTransactionPublisher;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.Objects;

/** 使用 RocketMQ Spring Starter 发送“订单已创建”事务消息。 */
@Service
public class OrderTransactionMessageProducer implements OrderTransactionPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderTransactionMessageProducer.class);

    private final RocketMQTemplate rocketMQTemplate;
    private final String destination;

    public OrderTransactionMessageProducer(
            RocketMQTemplate rocketMQTemplate,
            @Value("${demo.rocketmq.order-topic}") String topic,
            @Value("${demo.rocketmq.order-created-tag}") String tag) {
        this.rocketMQTemplate = rocketMQTemplate;
        this.destination = topic + ":" + tag;
    }

    /**
     * 先向 Broker 发送消费者不可见的半消息，再由 Starter 回调本地事务监听器。
     *
     * @param order 尚未写入数据库的订单候选对象
     * @return 包含 Broker 发送状态和本地事务状态的结果
     */
    @Override
    public OrderTransactionOutcome sendCreateOrderTransaction(PurchaseOrder order) {
        Objects.requireNonNull(order, "order 不能为空");
        if (order.id() != null) {
            throw new IllegalArgumentException("发送创建订单事务消息前，order.id 必须为空");
        }

        OrderCreatedMessage event = new OrderCreatedMessage(
                order.eventId(),
                order.orderNo(),
                order.productId(),
                order.quantity(),
                order.createdAt()
        );

        Message<OrderCreatedMessage> message = MessageBuilder
                .withPayload(event)
                // KEYS 便于运维人员通过订单号查询 RocketMQ 消息。
                .setHeader(RocketMQHeaders.KEYS, order.orderNo())
                // 以下两个业务 Header 会随半消息保存，供 Broker 回查使用。
                .setHeader(OrderTransactionHeaders.ORDER_NO, order.orderNo())
                .setHeader(OrderTransactionHeaders.EVENT_ID, order.eventId())
                .build();

        try (MessageLogContext ignored = MessageLogContext.forSend(
                "ORDER_CREATED_TRANSACTION",
                order.eventId(),
                order.orderNo())) {
            log.info("事务半消息发送开始，destination={}", destination);
            try {
                TransactionSendResult result = rocketMQTemplate.sendMessageInTransaction(
                        destination,
                        message,
                        new CreateOrderTransactionContext(order)
                );
                RocketMqSendResultValidator.requireSendOk(destination, result);
                log.info(
                        "事务消息发送完成，destination={}, msgId={}, sendStatus={}, localState={}",
                        destination,
                        result.getMsgId(),
                        result.getSendStatus(),
                        result.getLocalTransactionState()
                );
                return new OrderTransactionOutcome(
                        result.getMsgId(),
                        mapLocalState(result.getLocalTransactionState())
                );
            } catch (RuntimeException exception) {
                log.error("事务消息发送失败，destination={}", destination, exception);
                throw exception;
            }
        }
    }

    /** RocketMQ 状态只在消息适配层出现，转换后再交给订单应用 Service。 */
    private OrderTransactionOutcome.LocalState mapLocalState(LocalTransactionState state) {
        if (state == LocalTransactionState.COMMIT_MESSAGE) {
            return OrderTransactionOutcome.LocalState.COMMITTED;
        }
        if (state == LocalTransactionState.ROLLBACK_MESSAGE) {
            return OrderTransactionOutcome.LocalState.ROLLED_BACK;
        }
        return OrderTransactionOutcome.LocalState.UNKNOWN;
    }

}
