package com.example.rocketmq.flow;

import com.example.rocketmq.RocketMqLearningApplication;
import com.example.rocketmq.inventory.domain.Inventory;
import com.example.rocketmq.inventory.repository.InventoryRepository;
import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.repository.OrderRepository;
import com.example.rocketmq.order.service.TransactionalOrderService;
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
 * 真实 MySQL + RocketMQ 事务消息验收：半消息提交后才触发库存扣减。
 *
 * <pre>
 * Producer 发半消息（消费者不可见）
 *     -> 本地事务 INSERT 订单
 *     -> COMMIT 半消息
 *     -> 库存 Consumer 收到订单事件并扣减库存
 * </pre>
 */
@Tag("integration")
@SpringBootTest(
        classes = RocketMqLearningApplication.class,
        properties = {
                "rocketmq.name-server=127.0.0.1:9876",
                "rocketmq.producer.group=order-transaction-flow-it-producer-group",
                "demo.rocketmq.order-topic=order-event-topic",
                "demo.rocketmq.order-created-tag=order-transaction-flow-it",
                "demo.rocketmq.inventory-consumer-group=inventory-transaction-flow-it-consumer-group"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OrderTransactionFlowIT {

    private static final String CONSUMER_GROUP = "inventory-transaction-flow-it-consumer-group";
    private static final int INITIAL_STOCK = 12;
    private static final int ORDER_QUANTITY = 4;
    private static final int EXPECTED_STOCK = INITIAL_STOCK - ORDER_QUANTITY;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TransactionalOrderService transactionalOrderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private InventoryRepository inventoryRepository;

    private Long testProductId;
    private String testOrderNo;
    private String testEventId;

    @Test
    @Timeout(30)
    void shouldCommitOrderThenMakeTransactionalMessageVisibleToInventoryConsumer()
            throws InterruptedException {
        testProductId = ThreadLocalRandom.current().nextLong(
                6_000_000_000_000L,
                7_000_000_000_000L
        );
        insertCommittedInventory(testProductId, INITIAL_STOCK);

        PurchaseOrder createdOrder = transactionalOrderService.createOrder(
                testProductId,
                ORDER_QUANTITY
        );
        testOrderNo = createdOrder.orderNo();
        testEventId = createdOrder.eventId();

        PurchaseOrder storedOrder = orderRepository.findByOrderNo(testOrderNo)
                .orElseThrow(() -> new AssertionError("本地事务提交后未找到订单"));
        Inventory inventory = awaitInventory(
                testProductId,
                EXPECTED_STOCK,
                Duration.ofSeconds(10)
        );

        assertNotNull(createdOrder.id());
        assertEquals(createdOrder.id(), storedOrder.id());
        assertEquals(OrderStatus.CREATED, storedOrder.status());
        assertEquals(EXPECTED_STOCK, inventory.availableStock());
        assertEquals(1L, inventory.version());

        System.out.printf(
                "事务消息链路成功：orderNo=%s, stock=%d->%d, orderCommitted=true%n",
                testOrderNo,
                INITIAL_STOCK,
                inventory.availableStock()
        );
    }

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

    private Inventory awaitInventory(long productId, int expectedStock, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        Optional<Inventory> latest = Optional.empty();

        while (System.nanoTime() < deadline) {
            latest = inventoryRepository.findByProductId(productId);
            if (latest.isPresent() && latest.orElseThrow().availableStock() == expectedStock) {
                return latest.orElseThrow();
            }
            Thread.sleep(100);
        }

        throw new AssertionError(
                "等待事务消息扣减库存超时，productId=" + productId
                        + ", expectedStock=" + expectedStock
                        + ", actual=" + latest.map(Inventory::availableStock)
                        .map(String::valueOf)
                        .orElse("不存在")
        );
    }

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
            jdbcTemplate.update("DELETE FROM purchase_order WHERE order_no = ?", testOrderNo);
        }
        if (testProductId != null) {
            jdbcTemplate.update("DELETE FROM inventory WHERE product_id = ?", testProductId);
        }
    }
}
