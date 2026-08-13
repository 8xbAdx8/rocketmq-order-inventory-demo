package com.example.rocketmq.order.service;

import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
import com.example.rocketmq.order.service.port.OrderTransactionOutcome;
import com.example.rocketmq.order.service.port.OrderTransactionPublisher;
import com.example.rocketmq.order.repository.OrderRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 验证应用服务生成订单候选对象并返回本地事务持久化结果。 */
class TransactionalOrderServiceTest {

    @Test
    void shouldReturnOrderPersistedByLocalTransactionCallback() {
        OrderTransactionPublisher producer = mock(OrderTransactionPublisher.class);
        OrderRepository repository = mock(OrderRepository.class);
        TransactionalOrderService service = new TransactionalOrderService(producer, repository);
        OrderTransactionOutcome sendResult = new OrderTransactionOutcome(
                "msg-1001",
                OrderTransactionOutcome.LocalState.COMMITTED
        );
        when(producer.sendCreateOrderTransaction(org.mockito.ArgumentMatchers.any()))
                .thenReturn(sendResult);

        ArgumentCaptor<PurchaseOrder> orderCaptor = ArgumentCaptor.forClass(PurchaseOrder.class);
        /*
         * sendCreateOrderTransaction 内部同步执行监听器；在它返回后，数据库已经能查到该订单。
         * Mock 中用捕获到的订单构造一条带主键的持久化记录。
         */
        when(repository.findByOrderNo(org.mockito.ArgumentMatchers.anyString()))
                .thenAnswer(invocation -> {
                    PurchaseOrder candidate = orderCaptor.getValue();
                    return Optional.of(candidate.withId(99L));
                });

        // 先捕获 Producer 参数，再让 Repository Answer 使用同一个业务对象。
        when(producer.sendCreateOrderTransaction(orderCaptor.capture())).thenReturn(sendResult);

        PurchaseOrder created = service.createOrder(1001L, 3);

        verify(producer).sendCreateOrderTransaction(orderCaptor.getValue());
        assertNotNull(created.id());
        assertEquals(99L, created.id());
        assertEquals(1001L, created.productId());
        assertEquals(3, created.quantity());
        assertEquals(OrderStatus.CREATED, created.status());
    }
}
