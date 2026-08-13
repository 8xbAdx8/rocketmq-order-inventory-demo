package com.example.rocketmq.order.service.port;

/**
 * 订单应用层能够理解的事务消息发送结果。
 *
 * <p>它隔离 RocketMQ 的 {@code TransactionSendResult} 与 {@code LocalTransactionState}，
 * 避免订单 Service 直接依赖具体中间件 API。</p>
 */
public record OrderTransactionOutcome(
        String messageId,
        LocalState localState
) {

    public OrderTransactionOutcome {
        if (localState == null) {
            throw new IllegalArgumentException("localState 不能为空");
        }
    }

    public enum LocalState {
        COMMITTED,
        ROLLED_BACK,
        UNKNOWN
    }
}
