package com.example.rocketmq.order.exception;

/**
 * 按业务订单号找不到订单时抛出的业务异常。
 *
 * <p>异常本身不携带 HTTP 概念，既可以被 REST Controller 使用，
 * 也可以被定时任务、消息消费者或未来的 RPC 接口复用。</p>
 */
public class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(String orderNo) {
        super("订单不存在，orderNo=" + orderNo);
    }
}
