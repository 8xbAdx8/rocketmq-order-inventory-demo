package com.example.rocketmq.order.transaction;

/**
 * 事务消息自定义 Header。
 *
 * <p>Broker 回查发生时，原始 Java 参数已经不存在，因此必须把回查所需的业务标识放进消息属性。
 * 回查只需要订单号和事件 ID，不应把整个数据库实体塞进 Header。</p>
 */
public final class OrderTransactionHeaders {

    public static final String ORDER_NO = "transaction-order-no";
    public static final String EVENT_ID = "transaction-event-id";

    private OrderTransactionHeaders() {
    }
}
