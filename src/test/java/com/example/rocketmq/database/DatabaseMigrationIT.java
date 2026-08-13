package com.example.rocketmq.database;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Flyway 与真实 MySQL 的集成测试。
 *
 * <p>{@link JdbcTest} 只加载数据库相关组件，不启动 RocketMQ Producer 和 Consumer，
 * 可以更快、更准确地定位数据库配置问题。</p>
 *
 * <p>类名以 IT 结尾，普通 {@code mvn test} 不会自动运行。执行前需要通过
 * {@code MYSQL_USERNAME} 和 {@code MYSQL_PASSWORD} 提供本地 MySQL 凭据。</p>
 */
@Tag("integration")
@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class DatabaseMigrationIT {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void shouldApplyFlywayMigrationAndCreateBusinessTables() {
        assertEquals(1, countCurrentSchemaTable("purchase_order"));
        assertEquals(1, countCurrentSchemaTable("inventory"));
        assertEquals(1, countCurrentSchemaTable("order_lifecycle_event_log"));
        assertEquals(1, countCurrentSchemaTable("message_consume_record"));
        assertEquals(1, countCurrentSchemaTable("flyway_schema_history"));
    }

    /**
     * 通过 information_schema 查询，不写入任何业务数据。
     */
    private Integer countCurrentSchemaTable(String tableName) {
        return jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                  AND table_name = ?
                """,
                Integer.class,
                tableName
        );
    }
}
