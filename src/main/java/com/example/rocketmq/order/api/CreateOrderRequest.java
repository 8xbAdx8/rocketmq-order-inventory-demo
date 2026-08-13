package com.example.rocketmq.order.api;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/**
 * 创建订单的 HTTP 请求对象。
 *
 * <p>{@code @NotNull} 拒绝缺少字段，{@code @Positive} 拒绝 0 和负数。
 * Controller 上的 {@code @Valid} 会触发这些约束。</p>
 */
public record CreateOrderRequest(
        @NotNull(message = "productId 不能为空")
        @Positive(message = "productId 必须大于 0")
        Long productId,

        @NotNull(message = "quantity 不能为空")
        @Positive(message = "quantity 必须大于 0")
        Integer quantity
) {
}
