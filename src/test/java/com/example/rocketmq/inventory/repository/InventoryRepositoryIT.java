package com.example.rocketmq.inventory.repository;

import com.example.rocketmq.inventory.domain.Inventory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 原子扣减 SQL 与真实 MySQL 的集成测试。
 *
 * <p>JdbcTest 默认回滚，因此测试数据不会残留在库存表。</p>
 */
@Tag("integration")
@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(InventoryRepository.class)
class InventoryRepositoryIT {

    private static final AtomicLong PRODUCT_ID_SEQUENCE =
            new AtomicLong(9_000_000_000_000L);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private InventoryRepository inventoryRepository;

    @Test
    void shouldDeductStockAndIncreaseVersion() {
        long productId = PRODUCT_ID_SEQUENCE.incrementAndGet();
        insertInventory(productId, 10);

        int affectedRows = inventoryRepository.deductStock(productId, 4);
        Inventory inventory = inventoryRepository.findByProductId(productId).orElseThrow();

        assertEquals(1, affectedRows);
        assertEquals(6, inventory.availableStock());
        assertEquals(1L, inventory.version());
    }

    @Test
    void shouldNotChangeStockWhenAvailableStockIsInsufficient() {
        long productId = PRODUCT_ID_SEQUENCE.incrementAndGet();
        insertInventory(productId, 3);

        int affectedRows = inventoryRepository.deductStock(productId, 5);
        Inventory inventory = inventoryRepository.findByProductId(productId).orElseThrow();

        assertEquals(0, affectedRows);
        assertEquals(3, inventory.availableStock());
        assertEquals(0L, inventory.version());
    }

    private void insertInventory(long productId, int availableStock) {
        Instant now = Instant.parse("2026-08-09T10:00:00Z");
        jdbcTemplate.update(
                """
                INSERT INTO inventory
                    (product_id, available_stock, version, created_at, updated_at)
                VALUES (?, ?, 0, ?, ?)
                """,
                productId,
                availableStock,
                Timestamp.from(now),
                Timestamp.from(now)
        );
    }
}
