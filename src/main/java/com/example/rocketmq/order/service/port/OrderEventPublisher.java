package com.example.rocketmq.order.service.port;

import com.example.rocketmq.order.domain.PurchaseOrder;

/** 订单应用层发布“订单创建/超时检查”事件的出口端口。 */
public interface OrderEventPublisher {

    void publishOrderCreated(PurchaseOrder order);

    void publishOrderTimeoutCheck(PurchaseOrder order);
}
