package com.example.rocketmq.order.domain;

import java.time.Instant;
import java.util.Objects;

/**
 * 订单领域对象。
 *
 * <p>它表示订单模块中的业务状态，与发送给库存模块的
 * {@code OrderCreatedMessage} 是两个不同对象：前者用于持久化和业务计算，
 * 后者是跨模块消息契约。</p>
 *
 * @param id        数据库主键；持久化前允许为 null，保存后必须有值
 * @param orderNo   对外业务订单号
 * @param eventId   与本次订单创建对应的业务事件 ID
 * @param productId 商品 ID
 * @param quantity  购买数量
 * @param status    当前订单状态
 * @param createdAt UTC 创建时间
 * @param updatedAt UTC 更新时间
 */
public record PurchaseOrder(
        Long id,
        String orderNo,
        String eventId,
        Long productId,
        Integer quantity,
        OrderStatus status,
        Instant createdAt,
        Instant updatedAt
) {

    public PurchaseOrder {
        if (id != null && id <= 0) {
            throw new IllegalArgumentException("id 必须大于 0");
        }
        if (orderNo == null || orderNo.isBlank()) {
            throw new IllegalArgumentException("orderNo 不能为空");
        }
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId 不能为空");
        }

        Objects.requireNonNull(productId, "productId 不能为空");
        if (productId <= 0) {
            throw new IllegalArgumentException("productId 必须大于 0");
        }

        Objects.requireNonNull(quantity, "quantity 不能为空");
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity 必须大于 0");
        }

        Objects.requireNonNull(status, "status 不能为空");
        Objects.requireNonNull(createdAt, "createdAt 不能为空");
        Objects.requireNonNull(updatedAt, "updatedAt 不能为空");
    }

    /**
     * 返回带数据库主键的新对象，不修改原对象。
     */
    public PurchaseOrder withId(long generatedId) {
        return new PurchaseOrder(
                generatedId,
                orderNo,
                eventId,
                productId,
                quantity,
                status,
                createdAt,
                updatedAt
        );
    }
}
