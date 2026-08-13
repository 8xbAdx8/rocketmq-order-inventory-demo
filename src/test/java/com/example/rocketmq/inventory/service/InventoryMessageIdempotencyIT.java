package com.example.rocketmq.inventory.service;

import com.example.rocketmq.infrastructure.messaging.ConsumedMessageRepository;
import com.example.rocketmq.inventory.domain.Inventory;
import com.example.rocketmq.inventory.exception.InventoryNotFoundException;
import com.example.rocketmq.inventory.repository.InventoryRepository;
import com.example.rocketmq.order.message.OrderCreatedMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 使用真实 MySQL 验证幂等记录和库存扣减的原子性，不需要启动 RocketMQ。
 */
@Tag("integration")
@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({
        ConsumedMessageRepository.class,
        InventoryRepository.class,
        InventoryService.class,
        InventoryMessageConsumeService.class
})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class InventoryMessageIdempotencyIT {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private InventoryRepository inventoryRepository;

    @Autowired
    private ConsumedMessageRepository consumedMessageRepository;

    @Autowired
    private InventoryMessageConsumeService messageConsumeService;

    private Long testProductId;
    private String testConsumerGroup;
    private String testEventId;

    @Test
    void shouldDeductStockOnlyOnceForDuplicateBusinessEvent() {
        prepareUniqueKeys();
        insertInventory(testProductId, 10);
        OrderCreatedMessage message = message(testProductId, 3);

        MessageConsumeResult first = messageConsumeService.consumeOrderCreated(
                testConsumerGroup,
                message
        );
        MessageConsumeResult duplicate = messageConsumeService.consumeOrderCreated(
                testConsumerGroup,
                message
        );

        Inventory inventory = inventoryRepository.findByProductId(testProductId).orElseThrow();
        assertEquals(MessageConsumeResult.PROCESSED, first);
        assertEquals(MessageConsumeResult.DUPLICATE_SKIPPED, duplicate);
        assertEquals(7, inventory.availableStock());
        assertEquals(1L, inventory.version());
        assertEquals(1L, consumedMessageRepository.count(testConsumerGroup, testEventId));

        System.out.printf(
                "消费幂等成功：eventId=%s, deliveries=2, stock=10->%d, consumeRecords=1%n",
                testEventId,
                inventory.availableStock()
        );
    }

    @Test
    void shouldRollbackConsumeRecordWhenStockDeductionFails() {
        prepareUniqueKeys();
        OrderCreatedMessage message = message(testProductId, 1);

        assertThrows(
                InventoryNotFoundException.class,
                () -> messageConsumeService.consumeOrderCreated(testConsumerGroup, message)
        );

        // 库存业务失败时，先插入的幂等记录必须随同一个事务回滚。
        assertEquals(0L, consumedMessageRepository.count(testConsumerGroup, testEventId));
    }

    private void prepareUniqueKeys() {
        testProductId = ThreadLocalRandom.current().nextLong(
                4_000_000_000_000L,
                5_000_000_000_000L
        );
        String suffix = UUID.randomUUID().toString();
        testConsumerGroup = "idempotency-it-" + suffix;
        testEventId = "event-" + suffix;
    }

    private OrderCreatedMessage message(long productId, int quantity) {
        return new OrderCreatedMessage(
                testEventId,
                "ORD-" + UUID.randomUUID(),
                productId,
                quantity,
                Instant.now()
        );
    }

    private void insertInventory(long productId, int stock) {
        Instant now = Instant.now();
        jdbcTemplate.update(
                """
                INSERT INTO inventory
                    (product_id, available_stock, version, created_at, updated_at)
                VALUES (?, ?, 0, ?, ?)
                """,
                productId,
                stock,
                Timestamp.from(now),
                Timestamp.from(now)
        );
    }

    @AfterEach
    void cleanUp() {
        if (testConsumerGroup != null && testEventId != null) {
            jdbcTemplate.update(
                    "DELETE FROM message_consume_record WHERE consumer_group = ? AND event_id = ?",
                    testConsumerGroup,
                    testEventId
            );
        }
        if (testProductId != null) {
            jdbcTemplate.update("DELETE FROM inventory WHERE product_id = ?", testProductId);
        }
    }
}
