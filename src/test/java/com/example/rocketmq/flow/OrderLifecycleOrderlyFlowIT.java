package com.example.rocketmq.flow;

import com.example.rocketmq.RocketMqLearningApplication;
import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.message.OrderLifecycleEventType;
import com.example.rocketmq.order.message.OrderLifecycleMessage;
import com.example.rocketmq.order.message.OrderLifecycleMessageProducer;
import com.example.rocketmq.order.repository.OrderLifecycleEventRepository;
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
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 同一订单 CREATED → PAID → SHIPPED 顺序消息端到端测试。
 */
@Tag("integration")
@SpringBootTest(
        classes = RocketMqLearningApplication.class,
        properties = {
                "rocketmq.name-server=127.0.0.1:9876",
                "rocketmq.producer.group=order-lifecycle-flow-it-producer-group",
                "demo.rocketmq.order-topic=order-event-topic",
                "demo.rocketmq.order-lifecycle-tag=order-lifecycle-flow-it",
                "demo.rocketmq.order-lifecycle-consumer-group=order-lifecycle-flow-it-consumer-group"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OrderLifecycleOrderlyFlowIT {

    private static final List<OrderLifecycleEventType> EXPECTED_ORDER = List.of(
            OrderLifecycleEventType.CREATED,
            OrderLifecycleEventType.PAID,
            OrderLifecycleEventType.SHIPPED
    );

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OrderLifecycleMessageProducer producer;

    @Autowired
    private OrderLifecycleEventRepository lifecycleRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private String testOrderNo;

    @Test
    @Timeout(30)
    void shouldConsumeSameOrderLifecycleEventsInOrder() throws InterruptedException {
        String uniqueValue = UUID.randomUUID().toString();
        Instant now = Instant.now();
        PurchaseOrder savedOrder = orderRepository.save(new PurchaseOrder(
                null,
                "ORD-SEQUENCE-" + uniqueValue,
                "EVENT-SEQUENCE-" + uniqueValue,
                1001L,
                1,
                OrderStatus.CREATED,
                now,
                now
        ));
        testOrderNo = savedOrder.orderNo();

        producer.send(message(uniqueValue + "-1", OrderLifecycleEventType.CREATED, now));
        producer.send(message(uniqueValue + "-2", OrderLifecycleEventType.PAID, now.plusSeconds(1)));
        producer.send(message(uniqueValue + "-3", OrderLifecycleEventType.SHIPPED, now.plusSeconds(2)));

        List<OrderLifecycleEventType> actualOrder = awaitLifecycleOrder(
                testOrderNo,
                EXPECTED_ORDER,
                Duration.ofSeconds(15)
        );

        assertEquals(EXPECTED_ORDER, actualOrder);
        System.out.printf(
                "顺序消息链路成功：orderNo=%s, events=%s%n",
                testOrderNo,
                actualOrder
        );
    }

    private OrderLifecycleMessage message(
            String eventId,
            OrderLifecycleEventType eventType,
            Instant occurredAt) {
        return new OrderLifecycleMessage(
                eventId,
                testOrderNo,
                eventType,
                occurredAt
        );
    }

    private List<OrderLifecycleEventType> awaitLifecycleOrder(
            String orderNo,
            List<OrderLifecycleEventType> expected,
            Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        List<OrderLifecycleEventType> latest = List.of();

        while (System.nanoTime() < deadline) {
            latest = lifecycleRepository.findEventTypesByOrderNo(orderNo);
            if (latest.equals(expected)) {
                return latest;
            }
            Thread.sleep(100);
        }

        throw new AssertionError(
                "等待顺序消息消费超时，orderNo=" + orderNo
                        + ", expected=" + expected
                        + ", actual=" + latest
        );
    }

    @AfterEach
    void cleanUpTestData() {
        if (testOrderNo != null) {
            jdbcTemplate.update(
                    "DELETE FROM order_lifecycle_event_log WHERE order_no = ?",
                    testOrderNo
            );
            jdbcTemplate.update(
                    "DELETE FROM purchase_order WHERE order_no = ?",
                    testOrderNo
            );
        }
    }
}
