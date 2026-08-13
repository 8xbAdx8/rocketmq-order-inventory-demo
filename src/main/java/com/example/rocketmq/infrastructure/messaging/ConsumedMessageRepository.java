package com.example.rocketmq.infrastructure.messaging;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

/** 消息消费幂等记录的数据访问层。 */
@Repository
public class ConsumedMessageRepository {

    private static final String INSERT_IGNORE_SQL = """
            INSERT IGNORE INTO message_consume_record
                (consumer_group, event_id, message_type, business_key, consumed_at)
            VALUES (?, ?, ?, ?, ?)
            """;

    private static final String SELECT_BUSINESS_KEY_SQL = """
            SELECT business_key
            FROM message_consume_record
            WHERE consumer_group = ? AND event_id = ?
            """;

    private static final String COUNT_SQL = """
            SELECT COUNT(*)
            FROM message_consume_record
            WHERE consumer_group = ? AND event_id = ?
            """;

    private final JdbcTemplate jdbcTemplate;

    public ConsumedMessageRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 尝试占用一个幂等键。
     *
     * <p>MySQL 唯一键负责解决并发竞争：两个线程同时处理同一事件时，只有一个 INSERT
     * 能影响一行。该 INSERT 必须与后续业务更新处于同一个事务，业务失败时记录也会回滚。</p>
     *
     * @return true 表示首次消费；false 表示该 Group 已处理过该 eventId
     */
    public boolean tryRecord(
            String consumerGroup,
            String eventId,
            String messageType,
            String businessKey,
            Instant consumedAt) {
        requireText(consumerGroup, "consumerGroup");
        requireText(eventId, "eventId");
        requireText(messageType, "messageType");
        requireText(businessKey, "businessKey");
        if (consumedAt == null) {
            throw new IllegalArgumentException("consumedAt 不能为空");
        }

        int affectedRows = jdbcTemplate.update(
                INSERT_IGNORE_SQL,
                consumerGroup,
                eventId,
                messageType,
                businessKey,
                Timestamp.from(consumedAt)
        );
        if (affectedRows < 0 || affectedRows > 1) {
            throw new IllegalStateException("消费记录影响行数异常：" + affectedRows);
        }
        return affectedRows == 1;
    }

    public Optional<String> findBusinessKey(String consumerGroup, String eventId) {
        requireText(consumerGroup, "consumerGroup");
        requireText(eventId, "eventId");
        return jdbcTemplate.queryForList(
                        SELECT_BUSINESS_KEY_SQL,
                        String.class,
                        consumerGroup,
                        eventId
                )
                .stream()
                .findFirst();
    }

    /** 仅供测试和运维核对消费记录，不参与正常消费路径。 */
    public long count(String consumerGroup, String eventId) {
        Long count = jdbcTemplate.queryForObject(COUNT_SQL, Long.class, consumerGroup, eventId);
        return count == null ? 0 : count;
    }

    private void requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " 不能为空");
        }
    }
}
