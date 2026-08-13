package com.example.rocketmq.order.service.port;

import com.example.rocketmq.order.message.OrderLifecycleEventType;

/** 订单应用层发布生命周期事件的出口端口。 */
public interface OrderLifecyclePublisher {

    void publish(String orderNo, OrderLifecycleEventType eventType);
}
