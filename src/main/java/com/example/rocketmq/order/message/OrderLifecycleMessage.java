package com.example.rocketmq.order.message;

import java.time.Instant;
import java.util.Objects;

/**
 * 同一订单需要按顺序处理的生命周期消息契约。
 *
 * @param eventId   事件唯一标识
 * @param orderNo   业务订单号，同时作为 RocketMQ 队列选择键
 * @param eventType 生命周期事件类型
 * @param occurredAt 事件发生时间（UTC）
 */
public record OrderLifecycleMessage(
        String eventId,
        String orderNo,
        OrderLifecycleEventType eventType,
        Instant occurredAt
) {

    public OrderLifecycleMessage {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId 不能为空");
        }
        if (orderNo == null || orderNo.isBlank()) {
            throw new IllegalArgumentException("orderNo 不能为空");
        }
        Objects.requireNonNull(eventType, "eventType 不能为空");
        Objects.requireNonNull(occurredAt, "occurredAt 不能为空");
    }
}
