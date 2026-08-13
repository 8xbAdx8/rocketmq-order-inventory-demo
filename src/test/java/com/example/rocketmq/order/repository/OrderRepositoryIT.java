package com.example.rocketmq.order.repository;

import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Import;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OrderRepository 与真实 MySQL 的集成测试。
 *
 * <p>JdbcTest 默认在测试结束时回滚事务，因此测试订单不会残留在数据库。</p>
 */
@Tag("integration")
@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(OrderRepository.class)
class OrderRepositoryIT {

    @Autowired
    private OrderRepository orderRepository;

    @Test
    void shouldInsertAndFindOrderByOrderNo() {
        String uniqueValue = UUID.randomUUID().toString();
        Instant now = Instant.parse("2026-08-09T10:00:00.123Z");
        PurchaseOrder newOrder = new PurchaseOrder(
                null,
                "ORD-" + uniqueValue,
                "EVENT-" + uniqueValue,
                1001L,
                2,
                OrderStatus.CREATED,
                now,
                now
        );

        PurchaseOrder savedOrder = orderRepository.save(newOrder);
        Optional<PurchaseOrder> queriedOrder = orderRepository.findByOrderNo(newOrder.orderNo());

        assertNotNull(savedOrder.id());
        assertTrue(savedOrder.id() > 0);
        assertTrue(queriedOrder.isPresent());
        assertEquals(savedOrder.id(), queriedOrder.orElseThrow().id());
        assertEquals(newOrder.eventId(), queriedOrder.orElseThrow().eventId());
        assertEquals(OrderStatus.CREATED, queriedOrder.orElseThrow().status());
    }
}
