package com.example.rocketmq.order.api;

import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.service.OrderService;
import com.example.rocketmq.order.service.TransactionalOrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * 订单模块的 REST 适配层。
 *
 * <p>{@code @RestController} 表示返回值会由 Jackson 序列化成 JSON；
 * {@code @RequestMapping} 统一声明资源路径。Controller 只处理协议转换，
 * 真正的订单状态与消息编排仍由 Service 完成。</p>
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;
    private final TransactionalOrderService transactionalOrderService;

    public OrderController(
            OrderService orderService,
            TransactionalOrderService transactionalOrderService) {
        this.orderService = orderService;
        this.transactionalOrderService = transactionalOrderService;
    }

    /**
     * 普通消息流程：创建订单后发送库存、延迟取消和顺序生命周期消息。
     */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public OrderResponse createOrder(@Valid @RequestBody CreateOrderRequest request) {
        PurchaseOrder order = orderService.createOrder(request.productId(), request.quantity());
        return OrderResponse.from(order);
    }

    /**
     * 事务消息流程：先发送半消息，再在本地事务回调中写入订单。
     * 此接口专门用于对比普通消息与事务消息的一致性边界。
     */
    @PostMapping("/transactional")
    @ResponseStatus(HttpStatus.CREATED)
    public OrderResponse createTransactionalOrder(
            @Valid @RequestBody CreateOrderRequest request) {
        PurchaseOrder order = transactionalOrderService.createOrder(
                request.productId(),
                request.quantity()
        );
        return OrderResponse.from(order);
    }

    /** 按业务订单号查询当前状态。 */
    @GetMapping("/{orderNo}")
    public OrderResponse findOrder(@PathVariable String orderNo) {
        return OrderResponse.from(orderService.findOrder(orderNo));
    }

    /** 将 CREATED 订单推进为 PAID，并发送同一订单的顺序消息。 */
    @PostMapping("/{orderNo}/pay")
    public OrderResponse payOrder(@PathVariable String orderNo) {
        return OrderResponse.from(orderService.payOrder(orderNo));
    }

    /** 将 PAID 订单推进为 SHIPPED，并发送同一订单的顺序消息。 */
    @PostMapping("/{orderNo}/ship")
    public OrderResponse shipOrder(@PathVariable String orderNo) {
        return OrderResponse.from(orderService.shipOrder(orderNo));
    }
}
