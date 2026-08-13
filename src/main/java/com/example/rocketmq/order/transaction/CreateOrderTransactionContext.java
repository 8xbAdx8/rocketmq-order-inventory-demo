package com.example.rocketmq.order.transaction;

import com.example.rocketmq.order.domain.PurchaseOrder;

import java.util.Objects;

/**
 * 创建订单事务消息的本地上下文。
 *
 * <p>这个对象通过 {@code sendMessageInTransaction(..., arg)} 的 arg 参数传给本地事务监听器，
 * 只存在于当前生产者进程，不会作为消息正文发送给 Broker。真正跨服务传输的是
 * {@code OrderCreatedMessage}。</p>
 */
public record CreateOrderTransactionContext(PurchaseOrder order) {

    public CreateOrderTransactionContext {
        Objects.requireNonNull(order, "order 不能为空");
        if (order.id() != null) {
            throw new IllegalArgumentException("事务消息执行前，订单尚未持久化，id 必须为空");
        }
    }
}
