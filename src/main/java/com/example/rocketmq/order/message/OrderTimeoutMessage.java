package com.example.rocketmq.order.message;

import java.time.Instant;
import java.util.Objects;

/**
 * “订单到期后检查是否需要取消”的延迟消息契约。
 *
 * <p>消息只携带定位订单和追踪事件所需的最小数据。Consumer 收到消息后必须重新查询
 * 数据库中的最新订单状态，不能因为消息说“到期”就直接假设订单仍未支付。</p>
 *
 * @param eventId       本次超时检查事件的唯一标识
 * @param orderNo       需要检查的业务订单号
 * @param orderCreatedAt 订单创建时间，用于日志审计，而不是作为数据库更新条件
 */
public record OrderTimeoutMessage(
        String eventId,
        String orderNo,
        Instant orderCreatedAt
) {

    public OrderTimeoutMessage {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId 不能为空");
        }
        if (orderNo == null || orderNo.isBlank()) {
            throw new IllegalArgumentException("orderNo 不能为空");
        }
        Objects.requireNonNull(orderCreatedAt, "orderCreatedAt 不能为空");
    }
}
