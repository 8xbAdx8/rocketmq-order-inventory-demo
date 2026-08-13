package com.example.rocketmq.inventory.service;

import com.example.rocketmq.infrastructure.messaging.ConsumedMessageRepository;
import com.example.rocketmq.order.message.OrderCreatedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Objects;

/**
 * 库存模块的幂等消息处理服务。
 *
 * <p>“登记 eventId”和“扣减库存”在同一个本地事务中完成：两者一起提交或一起回滚。
 * 这是消费幂等最重要的事务边界。</p>
 */
@Service
public class InventoryMessageConsumeService {

    private static final Logger log = LoggerFactory.getLogger(InventoryMessageConsumeService.class);
    private static final String MESSAGE_TYPE = "ORDER_CREATED";

    private final ConsumedMessageRepository consumedMessageRepository;
    private final InventoryService inventoryService;

    public InventoryMessageConsumeService(
            ConsumedMessageRepository consumedMessageRepository,
            InventoryService inventoryService) {
        this.consumedMessageRepository = consumedMessageRepository;
        this.inventoryService = inventoryService;
    }

    /**
     * 幂等处理订单创建消息。
     *
     * <p>{@link Transactional} 使幂等记录与库存扣减共享 MySQL 事务。如果库存不足、记录不存在
     * 或数据库异常，整个事务回滚；RocketMQ 重试时仍有机会重新占用该 eventId。</p>
     */
    @Transactional
    public MessageConsumeResult consumeOrderCreated(
            String consumerGroup,
            OrderCreatedMessage message) {
        Objects.requireNonNull(message, "订单创建消息不能为空");

        boolean firstConsumption = consumedMessageRepository.tryRecord(
                consumerGroup,
                message.eventId(),
                MESSAGE_TYPE,
                message.orderNo(),
                Instant.now()
        );

        if (!firstConsumption) {
            String recordedOrderNo = consumedMessageRepository
                    .findBusinessKey(consumerGroup, message.eventId())
                    .orElseThrow(() -> new IllegalStateException(
                            "幂等键冲突后未找到原消费记录，eventId=" + message.eventId()
                    ));
            if (!recordedOrderNo.equals(message.orderNo())) {
                throw new IllegalStateException(
                        "同一 eventId 对应了不同订单，eventId=" + message.eventId()
                                + ", recordedOrderNo=" + recordedOrderNo
                                + ", currentOrderNo=" + message.orderNo()
                );
            }

            log.info(
                    "检测到重复订单消息，跳过库存扣减，consumerGroup={}, eventId={}, orderNo={}",
                    consumerGroup,
                    message.eventId(),
                    message.orderNo()
            );
            return MessageConsumeResult.DUPLICATE_SKIPPED;
        }

        inventoryService.deductStock(message.productId(), message.quantity());
        log.info(
                "订单消息首次处理完成，consumerGroup={}, eventId={}, orderNo={}",
                consumerGroup,
                message.eventId(),
                message.orderNo()
        );
        return MessageConsumeResult.PROCESSED;
    }
}
