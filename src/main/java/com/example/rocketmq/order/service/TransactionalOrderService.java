package com.example.rocketmq.order.service;

import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.service.port.OrderTransactionOutcome;
import com.example.rocketmq.order.service.port.OrderTransactionPublisher;
import com.example.rocketmq.order.repository.OrderRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

/**
 * 使用 RocketMQ 事务消息创建订单的应用服务。
 *
 * <p>与 {@link OrderService#createOrder(Long, Integer)} 的普通消息实现并存，便于学习时比较两种方案。
 * 本类不添加 {@code @Transactional}：真正的数据库事务由 RocketMQ 回调中的
 * {@code OrderLocalTransactionService} 管理。</p>
 */
@Service
public class TransactionalOrderService {

    private final OrderTransactionPublisher transactionPublisher;
    private final OrderRepository orderRepository;

    public TransactionalOrderService(
            OrderTransactionPublisher transactionPublisher,
            OrderRepository orderRepository) {
        this.transactionPublisher = transactionPublisher;
        this.orderRepository = orderRepository;
    }

    /** 创建订单；正常提交时返回已带数据库主键的订单。 */
    public PurchaseOrder createOrder(Long productId, Integer quantity) {
        validateRequest(productId, quantity);

        Instant now = Instant.now();
        PurchaseOrder candidate = new PurchaseOrder(
                null,
                generateOrderNo(),
                UUID.randomUUID().toString(),
                productId,
                quantity,
                OrderStatus.CREATED,
                now,
                now
        );

        OrderTransactionOutcome result = transactionPublisher
                .sendCreateOrderTransaction(candidate);

        if (result.localState() == OrderTransactionOutcome.LocalState.ROLLED_BACK) {
            throw new IllegalStateException("订单本地事务已回滚，orderNo=" + candidate.orderNo());
        }

        /*
         * COMMIT 时订单一定可查询；UNKNOWN 时也先查询数据库事实。
         * 若暂时查不到，不应伪装成功——Broker 仍会独立执行后续回查并决定消息命运。
         */
        return orderRepository.findByOrderNo(candidate.orderNo())
                .filter(order -> Objects.equals(order.eventId(), candidate.eventId()))
                .orElseThrow(() -> new IllegalStateException(
                                "订单事务状态尚待 Broker 确认，orderNo=" + candidate.orderNo()
                                + ", localState=" + result.localState()
                ));
    }

    private void validateRequest(Long productId, Integer quantity) {
        if (productId == null || productId <= 0) {
            throw new IllegalArgumentException("productId 必须大于 0");
        }
        if (quantity == null || quantity <= 0) {
            throw new IllegalArgumentException("quantity 必须大于 0");
        }
    }

    private String generateOrderNo() {
        return "TX-ORD-" + UUID.randomUUID()
                .toString()
                .replace("-", "")
                .toUpperCase(Locale.ROOT);
    }
}
