package com.example.rocketmq.order.message;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 订单创建事件的纯单元测试。
 *
 * <p>它不启动 Spring，也不连接 RocketMQ，只验证消息契约自身的规则，
 * 因此执行速度快，适合每次修改代码后运行。</p>
 */
class OrderCreatedMessageTest {

    @Test
    void shouldCreateMessageWhenFieldsAreValid() {
        Instant occurredAt = Instant.parse("2026-08-09T10:00:00Z");

        OrderCreatedMessage message = new OrderCreatedMessage(
                "event-1001",
                "ORDER-20260809-0001",
                101L,
                2,
                occurredAt
        );

        // record 会为每个字段生成同名访问方法，不需要手写 getter。
        assertEquals("event-1001", message.eventId());
        assertEquals("ORDER-20260809-0001", message.orderNo());
        assertEquals(101L, message.productId());
        assertEquals(2, message.quantity());
        assertEquals(occurredAt, message.occurredAt());
    }

    @Test
    void shouldRejectMessageWhenQuantityIsNotPositive() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new OrderCreatedMessage(
                        "event-1002",
                        "ORDER-20260809-0002",
                        101L,
                        0,
                        Instant.now()
                )
        );

        assertEquals("quantity 必须大于 0", exception.getMessage());
    }

    @Test
    void shouldRejectMessageWhenOrderNoIsBlank() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new OrderCreatedMessage(
                        "event-1003",
                        " ",
                        101L,
                        1,
                        Instant.now()
                )
        );

        assertEquals("orderNo 不能为空", exception.getMessage());
    }
}
