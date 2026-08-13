package com.example.rocketmq.order.transaction;

import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 事务消息对应的本地数据库事务。
 *
 * <p>它被单独做成 Spring Bean，是为了让 {@link Transactional} 代理真正生效：方法正常返回时提交，
 * 抛出运行时异常时回滚。事务监听器随后再把结果转换成 RocketMQ 的事务状态。</p>
 */
@Service
public class OrderLocalTransactionService {

    private final OrderRepository orderRepository;

    public OrderLocalTransactionService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    /** 在一个本地 MySQL 事务内插入订单。 */
    @Transactional
    public PurchaseOrder createOrder(PurchaseOrder order) {
        return orderRepository.save(order);
    }

    /**
     * Broker 回查时，以数据库中的持久化事实判断本地事务是否成功。
     * 同时比较 eventId，可以防止订单号被错误复用后产生误判。
     */
    @Transactional(readOnly = true)
    public boolean isCommitted(String orderNo, String eventId) {
        return orderRepository.findByOrderNo(orderNo)
                .map(PurchaseOrder::eventId)
                .filter(eventId::equals)
                .isPresent();
    }
}
