package com.example.rocketmq.order.repository;

import com.example.rocketmq.order.message.OrderLifecycleEventType;
import com.example.rocketmq.order.message.OrderLifecycleMessage;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

/**
 * 持久化订单生命周期消息的消费顺序。
 */
@Repository
public class OrderLifecycleEventRepository {

    private final JdbcTemplate jdbcTemplate;

    public OrderLifecycleEventRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void append(OrderLifecycleMessage message) {
        int affectedRows = jdbcTemplate.update(
                """
                INSERT INTO order_lifecycle_event_log
                    (event_id, order_no, event_type, occurred_at)
                VALUES (?, ?, ?, ?)
                """,
                message.eventId(),
                message.orderNo(),
                message.eventType().name(),
                Timestamp.from(message.occurredAt())
        );
        if (affectedRows != 1) {
            throw new IllegalStateException("订单生命周期事件保存失败：" + message.eventId());
        }
    }

    public Optional<OrderLifecycleEventType> findLatestEventType(String orderNo) {
        return jdbcTemplate.query(
                        """
                        SELECT event_type
                        FROM order_lifecycle_event_log
                        WHERE order_no = ?
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (resultSet, rowNum) -> OrderLifecycleEventType.valueOf(
                                resultSet.getString("event_type")
                        ),
                        orderNo
                )
                .stream()
                .findFirst();
    }

    public List<OrderLifecycleEventType> findEventTypesByOrderNo(String orderNo) {
        return jdbcTemplate.query(
                """
                SELECT event_type
                FROM order_lifecycle_event_log
                WHERE order_no = ?
                ORDER BY id ASC
                """,
                (resultSet, rowNum) -> OrderLifecycleEventType.valueOf(
                        resultSet.getString("event_type")
                ),
                orderNo
        );
    }
}
