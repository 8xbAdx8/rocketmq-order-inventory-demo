package com.example.rocketmq.order.message;

/**
 * 需要严格保持顺序的订单生命周期事件类型。
 */
public enum OrderLifecycleEventType {

    /** 订单已经创建。 */
    CREATED,

    /** 订单支付成功。 */
    PAID,

    /** 订单已经发货。 */
    SHIPPED
}
