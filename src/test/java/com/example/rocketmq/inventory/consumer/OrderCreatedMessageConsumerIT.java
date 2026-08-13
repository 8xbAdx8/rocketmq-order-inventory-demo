package com.example.rocketmq.inventory.consumer;

import com.example.rocketmq.inventory.service.InventoryMessageConsumeService;
import com.example.rocketmq.inventory.service.MessageConsumeResult;
import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.message.OrderCreatedMessage;
import com.example.rocketmq.order.message.OrderMessageProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 使用真实 Broker 验证：POJO 序列化 → Topic/Tag 路由 → Consumer 反序列化 → Service 调用。
 *
 * <p>幂等消费 Service 使用 Mock，所以不会连接 MySQL，也不会修改真实库存。</p>
 */
@Tag("integration")
@SpringBootTest(
        classes = OrderCreatedMessageConsumerIT.TestApplication.class,
        properties = {
                "rocketmq.name-server=127.0.0.1:9876",
                "rocketmq.producer.group=order-consumer-it-producer-group",
                "demo.rocketmq.order-topic=order-event-topic",
                // 测试 Tag 与正式 order-created 隔离。
                "demo.rocketmq.order-created-tag=order-created-consumer-it",
                "demo.rocketmq.inventory-consumer-group=inventory-consumer-it-group"
        }
)
class OrderCreatedMessageConsumerIT {

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {
            DataSourceAutoConfiguration.class,
            FlywayAutoConfiguration.class
    })
    @Import({
            OrderMessageProducer.class,
            OrderCreatedMessageConsumer.class
    })
    static class TestApplication {

        /**
         * 用 Mock 替代真实幂等消费 Service，使该测试只验证 RocketMQ 消息链路。
         */
        @Bean
        InventoryMessageConsumeService inventoryMessageConsumeService() {
            return mock(InventoryMessageConsumeService.class);
        }
    }

    @Autowired
    private OrderMessageProducer orderMessageProducer;

    @Autowired
    private InventoryMessageConsumeService messageConsumeService;

    @Test
    void shouldConsumeAndDeserializeOrderCreatedMessage() {
        String uniqueValue = UUID.randomUUID().toString();
        long productId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        Instant now = Instant.now();
        PurchaseOrder order = new PurchaseOrder(
                1L,
                "ORD-CONSUMER-IT-" + uniqueValue,
                "EVENT-CONSUMER-IT-" + uniqueValue,
                productId,
                2,
                OrderStatus.CREATED,
                now,
                now
        );
        when(messageConsumeService.consumeOrderCreated(
                eq("inventory-consumer-it-group"),
                org.mockito.ArgumentMatchers.any(OrderCreatedMessage.class)
        )).thenReturn(MessageConsumeResult.PROCESSED);

        SendResult sendResult = orderMessageProducer.sendOrderCreated(order);
        assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());

        // Consumer 在其他线程异步执行；最多等待 10 秒验证同一商品与数量已传给 Service。
        verify(messageConsumeService, timeout(10_000)).consumeOrderCreated(
                eq("inventory-consumer-it-group"),
                argThat(message -> message.eventId().equals(order.eventId())
                        && message.productId().equals(productId)
                        && message.quantity().equals(2))
        );
    }
}
