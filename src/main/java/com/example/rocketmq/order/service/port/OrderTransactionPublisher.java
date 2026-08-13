package com.example.rocketmq.order.service.port;

import com.example.rocketmq.order.domain.PurchaseOrder;

/** 创建订单事务事件的出口端口，屏蔽具体消息中间件。 */
public interface OrderTransactionPublisher {

    OrderTransactionOutcome sendCreateOrderTransaction(PurchaseOrder order);
}
