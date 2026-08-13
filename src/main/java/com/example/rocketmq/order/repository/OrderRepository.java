package com.example.rocketmq.order.repository;

import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Optional;

/**
 * 订单持久化仓库。
 *
 * <p>Repository 隐藏具体 SQL，让 Service 只关心“保存订单”和“查询订单”，
 * 而不关心 JDBC 连接、PreparedStatement 和结果集映射。</p>
 */
@Repository
public class OrderRepository {

    private static final String INSERT_SQL = """
            INSERT INTO purchase_order
                (order_no, event_id, product_id, quantity, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_BY_ORDER_NO_SQL = """
            SELECT id, order_no, event_id, product_id, quantity, status, created_at, updated_at
            FROM purchase_order
            WHERE order_no = ?
            """;

    private static final String TRANSITION_STATUS_SQL = """
            UPDATE purchase_order
            SET status = ?, updated_at = CURRENT_TIMESTAMP(3)
            WHERE order_no = ? AND status = ?
            """;

    /**
     * 将数据库的一行转换成领域对象。
     */
    private static final RowMapper<PurchaseOrder> ORDER_ROW_MAPPER = (resultSet, rowNum) ->
            new PurchaseOrder(
                    resultSet.getLong("id"),
                    resultSet.getString("order_no"),
                    resultSet.getString("event_id"),
                    resultSet.getLong("product_id"),
                    resultSet.getInt("quantity"),
                    OrderStatus.valueOf(resultSet.getString("status")),
                    resultSet.getTimestamp("created_at").toInstant(),
                    resultSet.getTimestamp("updated_at").toInstant()
            );

    private final JdbcTemplate jdbcTemplate;

    public OrderRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 插入订单并取得 MySQL 生成的自增主键。
     */
    public PurchaseOrder save(PurchaseOrder order) {
        if (order.id() != null) {
            throw new IllegalArgumentException("新增订单时 id 必须为空");
        }

        KeyHolder keyHolder = new GeneratedKeyHolder();

        int affectedRows = jdbcTemplate.update(connection -> {
            PreparedStatement statement = connection.prepareStatement(
                    INSERT_SQL,
                    Statement.RETURN_GENERATED_KEYS
            );
            statement.setString(1, order.orderNo());
            statement.setString(2, order.eventId());
            statement.setLong(3, order.productId());
            statement.setInt(4, order.quantity());
            // 枚举使用 name() 存储，避免 ordinal() 因枚举顺序变化而改变含义。
            statement.setString(5, order.status().name());
            statement.setTimestamp(6, Timestamp.from(order.createdAt()));
            statement.setTimestamp(7, Timestamp.from(order.updatedAt()));
            return statement;
        }, keyHolder);

        Number generatedKey = keyHolder.getKey();
        if (affectedRows != 1 || generatedKey == null) {
            throw new IllegalStateException("订单保存失败，未获得数据库主键");
        }

        return order.withId(generatedKey.longValue());
    }

    /**
     * 按业务订单号查询。Optional 明确表达“订单可能不存在”。
     */
    public Optional<PurchaseOrder> findByOrderNo(String orderNo) {
        if (orderNo == null || orderNo.isBlank()) {
            throw new IllegalArgumentException("orderNo 不能为空");
        }

        return jdbcTemplate.query(
                        SELECT_BY_ORDER_NO_SQL,
                        ORDER_ROW_MAPPER,
                        orderNo
                )
                .stream()
                .findFirst();
    }

    /**
     * 只取消仍处于 CREATED 状态的订单。
     *
     * <p>“检查状态”和“更新状态”必须在同一条 SQL 中完成。否则在先查后改的间隙，支付线程
     * 可能已经把订单改成 PAID，超时线程仍会错误地覆盖为 CANCELLED。</p>
     *
     * @return true 表示本次成功取消；false 表示订单不存在或已经不是 CREATED
     */
    public boolean cancelIfCreated(String orderNo) {
        return transitionStatus(orderNo, OrderStatus.CREATED, OrderStatus.CANCELLED);
    }

    /**
     * 使用一条条件 UPDATE 完成订单状态迁移，避免并发线程覆盖彼此的状态。
     */
    public boolean transitionStatus(
            String orderNo,
            OrderStatus expectedStatus,
            OrderStatus targetStatus) {
        if (orderNo == null || orderNo.isBlank()) {
            throw new IllegalArgumentException("orderNo 不能为空");
        }
        if (expectedStatus == null || targetStatus == null) {
            throw new IllegalArgumentException("订单状态不能为空");
        }

        int affectedRows = jdbcTemplate.update(
                TRANSITION_STATUS_SQL,
                targetStatus.name(),
                orderNo,
                expectedStatus.name()
        );
        if (affectedRows > 1) {
            throw new IllegalStateException("订单号唯一约束失效，更新了多条订单：" + orderNo);
        }
        return affectedRows == 1;
    }
}
