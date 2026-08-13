package com.example.rocketmq.order.api;

import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;

import java.time.Instant;

/**
 * 对外返回的订单视图。
 *
 * <p>API 不直接返回领域对象，可避免数据库结构或内部字段变化意外破坏外部契约。</p>
 */
public record OrderResponse(
        Long id,
        String orderNo,
        String eventId,
        Long productId,
        Integer quantity,
        OrderStatus status,
        Instant createdAt,
        Instant updatedAt
) {

    public static OrderResponse from(PurchaseOrder order) {
        return new OrderResponse(
                order.id(),
                order.orderNo(),
                order.eventId(),
                order.productId(),
                order.quantity(),
                order.status(),
                order.createdAt(),
                order.updatedAt()
        );
    }
}
