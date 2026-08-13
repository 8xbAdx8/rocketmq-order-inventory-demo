package com.example.rocketmq.inventory.repository;

import com.example.rocketmq.inventory.domain.Inventory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * 库存数据访问仓库。
 */
@Repository
public class InventoryRepository {

    /**
     * 条件扣减 SQL 是防止超卖的核心。
     *
     * <p>检查库存与扣减库存由 MySQL 在同一条 UPDATE 中原子完成。即使多个线程同时
     * 扣减同一商品，InnoDB 也会对目标行加锁，并在真正执行时重新判断 WHERE 条件。</p>
     */
    private static final String DEDUCT_STOCK_SQL = """
            UPDATE inventory
            SET available_stock = available_stock - ?,
                version = version + 1
            WHERE product_id = ?
              AND available_stock >= ?
            """;

    private static final String SELECT_BY_PRODUCT_ID_SQL = """
            SELECT product_id, available_stock, version, created_at, updated_at
            FROM inventory
            WHERE product_id = ?
            """;

    private static final RowMapper<Inventory> INVENTORY_ROW_MAPPER = (resultSet, rowNum) ->
            new Inventory(
                    resultSet.getLong("product_id"),
                    resultSet.getInt("available_stock"),
                    resultSet.getLong("version"),
                    resultSet.getTimestamp("created_at").toInstant(),
                    resultSet.getTimestamp("updated_at").toInstant()
            );

    private final JdbcTemplate jdbcTemplate;

    public InventoryRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 尝试原子扣减库存。
     *
     * @return 1 表示扣减成功；0 表示商品不存在或可用库存不足
     */
    public int deductStock(long productId, int quantity) {
        if (productId <= 0) {
            throw new IllegalArgumentException("productId 必须大于 0");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity 必须大于 0");
        }

        int affectedRows = jdbcTemplate.update(
                DEDUCT_STOCK_SQL,
                quantity,
                productId,
                quantity
        );

        // product_id 是主键，所以一条扣减语句不可能更新多行。
        if (affectedRows < 0 || affectedRows > 1) {
            throw new IllegalStateException("库存扣减影响行数异常：" + affectedRows);
        }
        return affectedRows;
    }

    public Optional<Inventory> findByProductId(long productId) {
        if (productId <= 0) {
            throw new IllegalArgumentException("productId 必须大于 0");
        }

        return jdbcTemplate.query(
                        SELECT_BY_PRODUCT_ID_SQL,
                        INVENTORY_ROW_MAPPER,
                        productId
                )
                .stream()
                .findFirst();
    }
}
