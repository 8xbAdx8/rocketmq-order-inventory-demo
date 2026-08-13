package com.example.rocketmq.order.message;

import java.time.Instant;
import java.util.Objects;

/**
 * “订单已创建”领域事件的消息契约。
 *
 * <p>这不是订单数据库实体，也不是 Controller 的请求参数。它只描述库存模块在收到
 * “订单已创建”事件后真正需要的数据，避免订单表结构变化直接影响消息消费者。</p>
 *
 * <p>使用 Java 17 的 {@code record} 可以得到不可变对象。消息一旦创建，其内容就不应
 * 在发送途中被修改；这也更符合“事件是已经发生的事实”这一含义。</p>
 *
 * @param eventId   事件唯一标识；后续用于消费幂等，不能直接使用 RocketMQ 的 msgId 代替
 * @param orderNo   业务订单号；用于关联订单，而不暴露数据库自增主键
 * @param productId 商品标识；库存模块据此找到需要扣减的商品库存
 * @param quantity  购买数量；必须大于 0
 * @param occurredAt 事件发生时间；使用 UTC 时间点，避免不同服务时区不一致
 */
public record OrderCreatedMessage(
        String eventId,
        String orderNo,
        Long productId,
        Integer quantity,
        Instant occurredAt
) {

    /**
     * record 的紧凑构造器：在事件对象创建的第一时间保护消息契约。
     *
     * <p>越早拒绝非法消息，越能避免坏数据进入 Broker 后被重复消费、重复重试。</p>
     */
    public OrderCreatedMessage {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId 不能为空");
        }
        if (orderNo == null || orderNo.isBlank()) {
            throw new IllegalArgumentException("orderNo 不能为空");
        }

        Objects.requireNonNull(productId, "productId 不能为空");
        if (productId <= 0) {
            throw new IllegalArgumentException("productId 必须大于 0");
        }

        Objects.requireNonNull(quantity, "quantity 不能为空");
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity 必须大于 0");
        }

        Objects.requireNonNull(occurredAt, "occurredAt 不能为空");
    }
}
