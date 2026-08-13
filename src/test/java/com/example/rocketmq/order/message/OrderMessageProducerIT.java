package com.example.rocketmq.order.message;

import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
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
import org.springframework.context.annotation.Import;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * OrderMessageProducer 与真实 RocketMQ Broker 的集成测试。
 *
 * <p>只加载消息转换器、RocketMQ 自动配置和被测 Producer，不加载完整应用，
 * 因此不需要 MySQL，也不会启动已有 Consumer。</p>
 */
@Tag("integration")
@SpringBootTest(
        classes = OrderMessageProducerIT.TestApplication.class,
        properties = {
                "rocketmq.name-server=127.0.0.1:9876",
                "rocketmq.producer.group=order-message-producer-it-group",
                "demo.rocketmq.order-topic=order-event-topic",
                // 使用测试 Tag，避免未来库存 Consumer 消费这条测试消息。
                "demo.rocketmq.order-created-tag=order-created-test"
        }
)
class OrderMessageProducerIT {

    /**
     * 最小测试应用：让 Spring Boot 正常发现 RocketMQ Starter 的内部自动配置，
     * 但不加载数据库自动配置，也不扫描项目中的其他 Service 和 Consumer。
     */
    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {
            DataSourceAutoConfiguration.class,
            FlywayAutoConfiguration.class
    })
    @Import(OrderMessageProducer.class)
    static class TestApplication {
    }

    @Autowired
    private OrderMessageProducer orderMessageProducer;

    @Test
    void shouldSendOrderCreatedMessageToRealBroker() {
        String uniqueValue = UUID.randomUUID().toString();
        Instant now = Instant.now();
        PurchaseOrder order = new PurchaseOrder(
                1L,
                "ORD-IT-" + uniqueValue,
                "EVENT-IT-" + uniqueValue,
                1001L,
                2,
                OrderStatus.CREATED,
                now,
                now
        );

        SendResult sendResult = orderMessageProducer.sendOrderCreated(order);

        assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
        assertNotNull(sendResult.getMsgId());
        System.out.printf(
                "订单事件发送成功：orderNo=%s, eventId=%s, msgId=%s%n",
                order.orderNo(),
                order.eventId(),
                sendResult.getMsgId()
        );
    }
}
