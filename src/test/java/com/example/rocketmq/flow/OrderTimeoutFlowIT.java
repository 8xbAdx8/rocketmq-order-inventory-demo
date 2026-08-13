package com.example.rocketmq.flow;

import com.example.rocketmq.RocketMqLearningApplication;
import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.message.OrderMessageProducer;
import com.example.rocketmq.order.repository.OrderRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 延迟消息端到端测试：测试环境把生产配置的 30 分钟覆盖为 3 秒。
 */
@Tag("integration")
@SpringBootTest(
        classes = RocketMqLearningApplication.class,
        properties = {
                "rocketmq.name-server=127.0.0.1:9876",
                "rocketmq.producer.group=order-timeout-flow-it-producer-group",
                "demo.rocketmq.order-topic=order-event-topic",
                "demo.rocketmq.order-created-tag=order-created-unused-timeout-flow-it",
                "demo.rocketmq.inventory-consumer-group=inventory-unused-timeout-flow-it-consumer-group",
                "demo.rocketmq.order-timeout-tag=order-timeout-flow-it",
                "demo.rocketmq.order-timeout-consumer-group=order-timeout-flow-it-consumer-group",
                "demo.rocketmq.order-timeout-delay-seconds=3"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OrderTimeoutFlowIT {

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OrderMessageProducer orderMessageProducer;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private String testOrderNo;

    @Test
    @Timeout(30)
    void shouldCancelCreatedOrderAfterDelay() throws InterruptedException {
        String uniqueValue = UUID.randomUUID().toString();
        Instant now = Instant.now();
        PurchaseOrder savedOrder = orderRepository.save(new PurchaseOrder(
                null,
                "ORD-TIMEOUT-" + uniqueValue,
                "EVENT-TIMEOUT-" + uniqueValue,
                1001L,
                1,
                OrderStatus.CREATED,
                now,
                now
        ));
        testOrderNo = savedOrder.orderNo();

        orderMessageProducer.sendOrderTimeoutCheck(savedOrder);

        PurchaseOrder cancelledOrder = awaitOrderStatus(
                testOrderNo,
                OrderStatus.CANCELLED,
                Duration.ofSeconds(15)
        );

        assertEquals(OrderStatus.CANCELLED, cancelledOrder.status());
        System.out.printf(
                "延迟消息链路成功：orderNo=%s, status=CREATED->CANCELLED%n",
                testOrderNo
        );
    }

    private PurchaseOrder awaitOrderStatus(
            String orderNo,
            OrderStatus expectedStatus,
            Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        Optional<PurchaseOrder> latestOrder = Optional.empty();

        while (System.nanoTime() < deadline) {
            latestOrder = orderRepository.findByOrderNo(orderNo);
            if (latestOrder.isPresent()
                    && latestOrder.orElseThrow().status() == expectedStatus) {
                return latestOrder.orElseThrow();
            }
            Thread.sleep(100);
        }

        throw new AssertionError(
                "等待订单超时取消失败，orderNo=" + orderNo
                        + ", expectedStatus=" + expectedStatus
                        + ", actual=" + latestOrder
                                .map(PurchaseOrder::status)
                                .map(Enum::name)
                                .orElse("不存在")
        );
    }

    @AfterEach
    void cleanUpTestData() {
        if (testOrderNo != null) {
            jdbcTemplate.update(
                    "DELETE FROM purchase_order WHERE order_no = ?",
                    testOrderNo
            );
        }
    }
}
