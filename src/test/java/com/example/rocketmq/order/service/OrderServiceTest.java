package com.example.rocketmq.order.service;

import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.exception.OrderNotFoundException;
import com.example.rocketmq.order.message.OrderLifecycleEventType;
import com.example.rocketmq.order.service.port.OrderEventPublisher;
import com.example.rocketmq.order.service.port.OrderLifecyclePublisher;
import com.example.rocketmq.order.repository.OrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * OrderService 的快速单元测试：不启动 Spring、不连接 MySQL、也不连接 RocketMQ。
 */
class OrderServiceTest {

    private OrderRepository orderRepository;
    private OrderEventPublisher orderEventPublisher;
    private OrderLifecyclePublisher lifecyclePublisher;
    private OrderService orderService;

    @BeforeEach
    void setUp() {
        orderRepository = mock(OrderRepository.class);
        orderEventPublisher = mock(OrderEventPublisher.class);
        lifecyclePublisher = mock(OrderLifecyclePublisher.class);
        orderService = new OrderService(
                orderRepository,
                orderEventPublisher,
                lifecyclePublisher
        );
    }

    @Test
    void shouldCreateAndSaveOrder() {
        // 模拟数据库返回自增主键，测试重点放在 Service 的业务编排。
        when(orderRepository.save(any(PurchaseOrder.class)))
                .thenAnswer(invocation -> {
                    PurchaseOrder order = invocation.getArgument(0);
                    return order.withId(1L);
                });

        PurchaseOrder result = orderService.createOrder(1001L, 2);

        ArgumentCaptor<PurchaseOrder> captor = ArgumentCaptor.forClass(PurchaseOrder.class);
        InOrder callOrder = inOrder(
                orderRepository,
                orderEventPublisher,
                lifecyclePublisher
        );
        callOrder.verify(orderRepository).save(captor.capture());
        callOrder.verify(orderEventPublisher).publishOrderCreated(result);
        callOrder.verify(orderEventPublisher).publishOrderTimeoutCheck(result);
        callOrder.verify(lifecyclePublisher).publish(
                result.orderNo(),
                OrderLifecycleEventType.CREATED
        );
        PurchaseOrder orderBeforeSave = captor.getValue();

        assertEquals(1L, result.id());
        assertEquals(1001L, result.productId());
        assertEquals(2, result.quantity());
        assertEquals(OrderStatus.CREATED, result.status());
        assertTrue(result.orderNo().startsWith("ORD-"));
        assertNotNull(result.eventId());
        // 传给 Repository 时还没有数据库自增主键。
        assertNull(orderBeforeSave.id());
    }

    @Test
    void shouldRejectInvalidQuantityBeforeAccessingDatabase() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> orderService.createOrder(1001L, 0)
        );

        assertEquals("quantity 必须大于 0", exception.getMessage());
        verify(orderRepository, never()).save(any(PurchaseOrder.class));
        verifyNoInteractions(orderEventPublisher);
        verifyNoInteractions(lifecyclePublisher);
    }

    @Test
    void shouldPropagateMessageSendFailure() {
        when(orderRepository.save(any(PurchaseOrder.class)))
                .thenAnswer(invocation -> {
                    PurchaseOrder order = invocation.getArgument(0);
                    return order.withId(2L);
                });
        doThrow(new IllegalStateException("模拟 Broker 不可用"))
                .when(orderEventPublisher)
                .publishOrderCreated(any(PurchaseOrder.class));

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> orderService.createOrder(1001L, 1)
        );

        assertEquals("模拟 Broker 不可用", exception.getMessage());
        verify(orderRepository).save(any(PurchaseOrder.class));
        verify(orderEventPublisher).publishOrderCreated(any(PurchaseOrder.class));
        verify(orderEventPublisher, never()).publishOrderTimeoutCheck(any(PurchaseOrder.class));
        verifyNoInteractions(lifecyclePublisher);
    }

    @Test
    void shouldPayCreatedOrderAndSendPaidEvent() {
        PurchaseOrder paidOrder = orderWithStatus(OrderStatus.PAID);
        when(orderRepository.transitionStatus(
                "ORD-1001",
                OrderStatus.CREATED,
                OrderStatus.PAID
        )).thenReturn(true);
        when(orderRepository.findByOrderNo("ORD-1001")).thenReturn(java.util.Optional.of(paidOrder));

        PurchaseOrder result = orderService.payOrder("ORD-1001");

        assertEquals(OrderStatus.PAID, result.status());
        verify(lifecyclePublisher).publish("ORD-1001", OrderLifecycleEventType.PAID);
    }

    @Test
    void shouldShipPaidOrderAndSendShippedEvent() {
        PurchaseOrder shippedOrder = orderWithStatus(OrderStatus.SHIPPED);
        when(orderRepository.transitionStatus(
                "ORD-1001",
                OrderStatus.PAID,
                OrderStatus.SHIPPED
        )).thenReturn(true);
        when(orderRepository.findByOrderNo("ORD-1001"))
                .thenReturn(java.util.Optional.of(shippedOrder));

        PurchaseOrder result = orderService.shipOrder("ORD-1001");

        assertEquals(OrderStatus.SHIPPED, result.status());
        verify(lifecyclePublisher).publish("ORD-1001", OrderLifecycleEventType.SHIPPED);
    }

    @Test
    void shouldFindOrderByBusinessOrderNumber() {
        PurchaseOrder order = orderWithStatus(OrderStatus.CREATED);
        when(orderRepository.findByOrderNo("ORD-1001"))
                .thenReturn(java.util.Optional.of(order));

        PurchaseOrder result = orderService.findOrder("ORD-1001");

        assertEquals(order, result);
    }

    @Test
    void shouldReportMissingOrder() {
        when(orderRepository.findByOrderNo("ORD-MISSING"))
                .thenReturn(java.util.Optional.empty());

        OrderNotFoundException exception = assertThrows(
                OrderNotFoundException.class,
                () -> orderService.findOrder("ORD-MISSING")
        );

        assertEquals("订单不存在，orderNo=ORD-MISSING", exception.getMessage());
    }

    private PurchaseOrder orderWithStatus(OrderStatus status) {
        java.time.Instant now = java.time.Instant.parse("2026-08-10T02:00:00Z");
        return new PurchaseOrder(
                1L,
                "ORD-1001",
                "event-1001",
                1001L,
                1,
                status,
                now,
                now
        );
    }
}
