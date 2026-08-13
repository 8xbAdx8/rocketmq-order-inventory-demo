package com.example.rocketmq.order.service;

import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * 处理订单超时策略的业务服务。
 */
@Service
public class OrderTimeoutService {

    private static final Logger log = LoggerFactory.getLogger(OrderTimeoutService.class);

    private final OrderRepository orderRepository;

    public OrderTimeoutService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    /**
     * 如果订单仍未支付，则将其取消。
     *
     * <p>延迟消息只代表“现在应该检查”，不代表“现在一定要取消”。订单可能已支付、
     * 已取消，消息也可能被重复投递，因此最终决定必须以数据库当前状态为准。</p>
     */
    @Transactional
    public boolean cancelIfUnpaid(String orderNo) {
        boolean cancelled = orderRepository.cancelIfCreated(orderNo);
        if (cancelled) {
            log.info("未支付订单超时取消成功，orderNo={}", orderNo);
            return true;
        }

        Optional<PurchaseOrder> currentOrder = orderRepository.findByOrderNo(orderNo);
        if (currentOrder.isEmpty()) {
            log.warn("超时检查未找到订单，忽略消息，orderNo={}", orderNo);
        } else {
            log.info(
                    "订单无需超时取消，orderNo={}, currentStatus={}",
                    orderNo,
                    currentOrder.orElseThrow().status()
            );
        }
        return false;
    }
}
