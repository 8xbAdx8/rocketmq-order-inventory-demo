package com.example.rocketmq.order.domain;

/**
 * 订单生命周期状态。
 *
 * <p>数据库保存枚举名称而不是序号。这样即使以后调整枚举声明顺序，
 * 已有数据的含义也不会发生变化。</p>
 */
public enum OrderStatus {

    /** 订单已创建，尚未支付。 */
    CREATED,

    /** 订单已支付。 */
    PAID,

    /** 订单已取消。 */
    CANCELLED,

    /** 订单已发货。 */
    SHIPPED
}
