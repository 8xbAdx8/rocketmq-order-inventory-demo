package com.example.rocketmq.order.service;

import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.exception.OrderNotFoundException;
import com.example.rocketmq.order.message.OrderLifecycleEventType;
import com.example.rocketmq.order.service.port.OrderEventPublisher;
import com.example.rocketmq.order.service.port.OrderLifecyclePublisher;
import com.example.rocketmq.order.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Locale;
import java.util.UUID;

/**
 * 订单应用服务，负责组织一次“创建订单”用例。
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OrderEventPublisher orderEventPublisher;
    private final OrderLifecyclePublisher lifecyclePublisher;

    public OrderService(
            OrderRepository orderRepository,
            OrderEventPublisher orderEventPublisher,
            OrderLifecyclePublisher lifecyclePublisher) {
        this.orderRepository = orderRepository;
        this.orderEventPublisher = orderEventPublisher;
        this.lifecyclePublisher = lifecyclePublisher;
    }

    /**
     * 创建并持久化订单。
     *
     * <p>{@link Transactional} 表示该方法在本地数据库事务中执行：方法正常结束就提交，
     * 抛出运行时异常就回滚。当前只有一次 INSERT，但先明确事务边界，便于下一步分析
     * 数据库事务与 RocketMQ 发送之间为什么无法天然保持原子性。</p>
     *
     * @param productId 商品 ID
     * @param quantity  购买数量
     * @return 已包含数据库主键的订单
     */
    @Transactional
    public PurchaseOrder createOrder(Long productId, Integer quantity) {
        if (productId == null || productId <= 0) {
            throw new IllegalArgumentException("productId 必须大于 0");
        }
        if (quantity == null || quantity <= 0) {
            throw new IllegalArgumentException("quantity 必须大于 0");
        }

        Instant now = Instant.now();
        PurchaseOrder newOrder = new PurchaseOrder(
                null,
                generateOrderNo(),
                UUID.randomUUID().toString(),
                productId,
                quantity,
                OrderStatus.CREATED,
                now,
                now
        );

        PurchaseOrder savedOrder = orderRepository.save(newOrder);

        /*
         * 必须使用保存后的订单构造消息，因为此时数据库主键已经生成。
         * 发送异常不在这里吞掉：异常继续抛出后，Spring 会尝试回滚当前数据库事务。
         * 但“Broker 成功、数据库提交失败”仍无法靠本地事务解决，这正是后续事务消息的主题。
         */
        orderEventPublisher.publishOrderCreated(savedOrder);

        // 延迟消息由 Broker 保存，30 分钟到期后才对超时 Consumer 可见。
        orderEventPublisher.publishOrderTimeoutCheck(savedOrder);

        // 生命周期消息使用 orderNo 选择队列，为后续 PAID、SHIPPED 建立同一条顺序链。
        lifecyclePublisher.publish(
                savedOrder.orderNo(),
                OrderLifecycleEventType.CREATED
        );

        log.info(
                "订单创建流程完成，orderNo={}, eventId={}, productId={}, quantity={}, status={}",
                savedOrder.orderNo(),
                savedOrder.eventId(),
                savedOrder.productId(),
                savedOrder.quantity(),
                savedOrder.status()
        );
        return savedOrder;
    }

    /**
     * 按业务订单号查询订单。
     *
     * <p>Repository 用 Optional 表达“可能不存在”；应用层将它转换为明确的业务异常，
     * HTTP 适配层再把该异常映射成 404。这样领域用例不需要依赖 Web API 的状态码。</p>
     */
    public PurchaseOrder findOrder(String orderNo) {
        return orderRepository.findByOrderNo(orderNo)
                .orElseThrow(() -> new OrderNotFoundException(orderNo));
    }

    /**
     * 将订单从 CREATED 原子更新为 PAID，并发送支付成功顺序消息。
     */
    @Transactional
    public PurchaseOrder payOrder(String orderNo) {
        return transitionOrder(
                orderNo,
                OrderStatus.CREATED,
                OrderStatus.PAID,
                OrderLifecycleEventType.PAID
        );
    }

    /**
     * 将订单从 PAID 原子更新为 SHIPPED，并发送发货顺序消息。
     */
    @Transactional
    public PurchaseOrder shipOrder(String orderNo) {
        return transitionOrder(
                orderNo,
                OrderStatus.PAID,
                OrderStatus.SHIPPED,
                OrderLifecycleEventType.SHIPPED
        );
    }

    private PurchaseOrder transitionOrder(
            String orderNo,
            OrderStatus expectedStatus,
            OrderStatus targetStatus,
            OrderLifecycleEventType eventType) {
        boolean transitioned = orderRepository.transitionStatus(
                orderNo,
                expectedStatus,
                targetStatus
        );
        if (!transitioned) {
            String currentStatus = orderRepository.findByOrderNo(orderNo)
                    .map(PurchaseOrder::status)
                    .map(Enum::name)
                    .orElse("NOT_FOUND");
            throw new IllegalStateException(
                    "订单状态迁移失败，orderNo=" + orderNo
                            + ", expected=" + expectedStatus
                            + ", target=" + targetStatus
                            + ", current=" + currentStatus
            );
        }

        PurchaseOrder transitionedOrder = orderRepository.findByOrderNo(orderNo)
                .orElseThrow(() -> new IllegalStateException("状态更新后订单不存在：" + orderNo));
        lifecyclePublisher.publish(orderNo, eventType);

        log.info(
                "订单状态迁移成功，orderNo={}, status={}->{}, eventType={}",
                orderNo,
                expectedStatus,
                targetStatus,
                eventType
        );
        return transitionedOrder;
    }

    /**
     * 教学项目使用 UUID 生成全局唯一业务订单号。
     * 企业项目通常会使用雪花算法、号段服务或专门的 ID 服务。
     */
    private String generateOrderNo() {
        return "ORD-" + UUID.randomUUID()
                .toString()
                .replace("-", "")
                .toUpperCase(Locale.ROOT);
    }
}
