package com.example.rocketmq.order.api;

import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.exception.OrderNotFoundException;
import com.example.rocketmq.order.service.OrderService;
import com.example.rocketmq.order.service.TransactionalOrderService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 只加载 Spring MVC 切片，不连接 MySQL 和 RocketMQ。
 * 测试重点是 JSON 契约、状态码、参数校验和 Controller 到 Service 的委托关系。
 */
@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OrderService orderService;

    @MockitoBean
    private TransactionalOrderService transactionalOrderService;

    @Test
    void shouldCreateOrderWithOrdinaryMessageFlow() throws Exception {
        when(orderService.createOrder(1001L, 2))
                .thenReturn(order("ORD-1001", OrderStatus.CREATED));

        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"productId":1001,"quantity":2}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderNo").value("ORD-1001"))
                .andExpect(jsonPath("$.productId").value(1001))
                .andExpect(jsonPath("$.quantity").value(2))
                .andExpect(jsonPath("$.status").value("CREATED"));

        verify(orderService).createOrder(1001L, 2);
    }

    @Test
    void shouldCreateOrderWithTransactionMessageFlow() throws Exception {
        when(transactionalOrderService.createOrder(1001L, 2))
                .thenReturn(order("TX-ORD-1001", OrderStatus.CREATED));

        mockMvc.perform(post("/api/orders/transactional")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"productId":1001,"quantity":2}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderNo").value("TX-ORD-1001"));

        verify(transactionalOrderService).createOrder(1001L, 2);
    }

    @Test
    void shouldRejectInvalidRequestBeforeCallingService() throws Exception {
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"productId":1001,"quantity":0}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("请求参数校验失败"))
                .andExpect(jsonPath("$.fieldErrors.quantity").value("quantity 必须大于 0"));
    }

    @Test
    void shouldReturnNotFoundForMissingOrder() throws Exception {
        when(orderService.findOrder("ORD-MISSING"))
                .thenThrow(new OrderNotFoundException("ORD-MISSING"));

        mockMvc.perform(get("/api/orders/ORD-MISSING"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.message")
                        .value("订单不存在，orderNo=ORD-MISSING"));
    }

    @Test
    void shouldReturnConflictForInvalidStateTransition() throws Exception {
        when(orderService.payOrder("ORD-1001"))
                .thenThrow(new IllegalStateException("订单当前状态不是 CREATED"));

        mockMvc.perform(post("/api/orders/ORD-1001/pay"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.message").value("订单当前状态不是 CREATED"));
    }

    private PurchaseOrder order(String orderNo, OrderStatus status) {
        Instant now = Instant.parse("2026-08-11T08:00:00Z");
        return new PurchaseOrder(
                1L,
                orderNo,
                "event-1001",
                1001L,
                2,
                status,
                now,
                now
        );
    }
}
