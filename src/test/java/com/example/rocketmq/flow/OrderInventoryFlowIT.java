package com.example.rocketmq.flow;

import com.example.rocketmq.RocketMqLearningApplication;
import com.example.rocketmq.inventory.domain.Inventory;
import com.example.rocketmq.inventory.repository.InventoryRepository;
import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.repository.OrderRepository;
import com.example.rocketmq.order.service.OrderService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 第三阶段完整业务链路验收：
 *
 * <pre>
 * 测试准备库存 → OrderService 写订单 → Producer 发消息 → Broker
 *              → Consumer 收消息 → InventoryService 原子扣库存
 * </pre>
 *
 * <p>这是同时依赖真实 MySQL 与真实 RocketMQ 的端到端集成测试。类名以 IT 结尾，
 * 普通 {@code mvn test} 不会自动执行。</p>
 */
@Tag("integration")
@SpringBootTest(
        classes = RocketMqLearningApplication.class,
        properties = {
                "rocketmq.name-server=127.0.0.1:9876",
                "rocketmq.producer.group=order-inventory-flow-it-producer-group",
                "demo.rocketmq.order-topic=order-event-topic",
                // 复用全部生产代码，但隔离测试消息和正式消息。
                "demo.rocketmq.order-created-tag=order-created-flow-it",
                "demo.rocketmq.inventory-consumer-group=inventory-flow-it-consumer-group"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OrderInventoryFlowIT {

    private static final String CONSUMER_GROUP = "inventory-flow-it-consumer-group";
    private static final int INITIAL_STOCK = 10;
    private static final int ORDER_QUANTITY = 3;
    private static final int EXPECTED_STOCK = INITIAL_STOCK - ORDER_QUANTITY;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private InventoryRepository inventoryRepository;

    /** 只记录本测试创建的唯一数据，供精确清理使用。 */
    private Long testProductId;
    private String testOrderNo;
    private String testEventId;

    @Test
    @Timeout(30)
    void shouldCreateOrderSendMessageAndDeductInventory() throws InterruptedException {
        testProductId = ThreadLocalRandom.current().nextLong(
                8_000_000_000_000L,
                9_000_000_000_000L
        );
        insertCommittedInventory(testProductId, INITIAL_STOCK);

        /*
         * OrderService 自己开启并提交数据库事务。方法内部先 INSERT 订单，
         * 再通过正式 OrderMessageProducer 发送消息。
         */
        PurchaseOrder createdOrder = orderService.createOrder(
                testProductId,
                ORDER_QUANTITY
        );
        testOrderNo = createdOrder.orderNo();
        testEventId = createdOrder.eventId();

        // 方法返回时 Spring 事务代理已经完成提交，可以从新查询中看到订单。
        PurchaseOrder storedOrder = orderRepository
                .findByOrderNo(testOrderNo)
                .orElseThrow(() -> new AssertionError("数据库中未找到刚创建的订单"));

        /*
         * Consumer 在 RocketMQ 消费线程中异步执行，不能用固定 sleep 猜时间。
         * 这里最多轮询 10 秒，一旦观察到目标库存就立即继续。
         */
        Inventory deductedInventory = awaitInventory(
                testProductId,
                EXPECTED_STOCK,
                Duration.ofSeconds(10)
        );

        assertNotNull(createdOrder.id());
        assertEquals(createdOrder.id(), storedOrder.id());
        assertEquals(OrderStatus.CREATED, storedOrder.status());
        assertEquals(testProductId, storedOrder.productId());
        assertEquals(ORDER_QUANTITY, storedOrder.quantity());
        assertEquals(EXPECTED_STOCK, deductedInventory.availableStock());
        assertEquals(1L, deductedInventory.version());

        System.out.printf(
                "完整链路成功：orderNo=%s, productId=%d, stock=%d->%d, version=%d%n",
                testOrderNo,
                testProductId,
                INITIAL_STOCK,
                deductedInventory.availableStock(),
                deductedInventory.version()
        );
    }

    /**
     * SpringBootTest 默认不包裹测试事务，因此该 INSERT 使用自动提交。
     * Consumer 的另一条数据库连接可以立即看到这条库存记录。
     */
    private void insertCommittedInventory(long productId, int availableStock) {
        Instant now = Instant.now();
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

    private Inventory awaitInventory(
            long productId,
            int expectedStock,
            Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        Optional<Inventory> latestInventory = Optional.empty();

        while (System.nanoTime() < deadline) {
            latestInventory = inventoryRepository.findByProductId(productId);
            if (latestInventory.isPresent()
                    && latestInventory.orElseThrow().availableStock() == expectedStock) {
                return latestInventory.orElseThrow();
            }
            Thread.sleep(100);
        }

        throw new AssertionError(
                "等待库存异步扣减超时，productId=" + productId
                        + ", expectedStock=" + expectedStock
                        + ", actual=" + latestInventory
                                .map(Inventory::availableStock)
                                .map(String::valueOf)
                                .orElse("不存在")
        );
    }

    /**
     * 不使用 DROP、TRUNCATE 或全表 DELETE，只按本测试生成的唯一键清理。
     */
    @AfterEach
    void cleanUpTestData() {
        if (testEventId != null) {
            jdbcTemplate.update(
                    "DELETE FROM message_consume_record WHERE consumer_group = ? AND event_id = ?",
                    CONSUMER_GROUP,
                    testEventId
            );
        }
        if (testOrderNo != null) {
            jdbcTemplate.update(
                    "DELETE FROM purchase_order WHERE order_no = ?",
                    testOrderNo
            );
        }
        if (testProductId != null) {
            jdbcTemplate.update(
                    "DELETE FROM inventory WHERE product_id = ?",
                    testProductId
            );
        }
    }
}
