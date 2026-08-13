package com.example.rocketmq.order.service;

import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.repository.OrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderTimeoutServiceTest {

    private OrderRepository orderRepository;
    private OrderTimeoutService orderTimeoutService;

    @BeforeEach
    void setUp() {
        orderRepository = mock(OrderRepository.class);
        orderTimeoutService = new OrderTimeoutService(orderRepository);
    }

    @Test
    void shouldCancelOrderThatIsStillCreated() {
        when(orderRepository.cancelIfCreated("ORD-1001")).thenReturn(true);

        assertTrue(orderTimeoutService.cancelIfUnpaid("ORD-1001"));

        verify(orderRepository).cancelIfCreated("ORD-1001");
    }

    @Test
    void shouldNotCancelOrderThatHasAlreadyBeenPaid() {
        when(orderRepository.cancelIfCreated("ORD-1001")).thenReturn(false);
        when(orderRepository.findByOrderNo("ORD-1001"))
                .thenReturn(Optional.of(paidOrder()));

        assertFalse(orderTimeoutService.cancelIfUnpaid("ORD-1001"));

        verify(orderRepository).findByOrderNo("ORD-1001");
    }

    private PurchaseOrder paidOrder() {
        Instant now = Instant.parse("2026-08-10T01:00:00Z");
        return new PurchaseOrder(
                1L,
                "ORD-1001",
                "event-1001",
                1001L,
                1,
                OrderStatus.PAID,
                now,
                now
        );
    }
}
