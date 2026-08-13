package com.example.rocketmq.order.transaction;

import com.example.rocketmq.order.domain.OrderStatus;
import com.example.rocketmq.order.domain.PurchaseOrder;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 覆盖本地事务执行结果以及 Broker 回查决策。 */
class OrderCreateTransactionListenerTest {

    private OrderLocalTransactionService localTransactionService;
    private OrderCreateTransactionListener listener;
    private PurchaseOrder order;

    @BeforeEach
    void setUp() {
        localTransactionService = mock(OrderLocalTransactionService.class);
        listener = new OrderCreateTransactionListener(localTransactionService);
        order = unsavedOrder();
    }

    @Test
    void shouldCommitHalfMessageAfterLocalOrderTransactionSucceeds() {
        RocketMQLocalTransactionState state = listener.executeLocalTransaction(
                transactionMessage(),
                new CreateOrderTransactionContext(order)
        );

        assertEquals(RocketMQLocalTransactionState.COMMIT, state);
        verify(localTransactionService).createOrder(order);
    }

    @Test
    void shouldRollbackHalfMessageForDefiniteBusinessFailure() {
        doThrow(new DuplicateKeyException("duplicate order"))
                .when(localTransactionService).createOrder(order);

        RocketMQLocalTransactionState state = listener.executeLocalTransaction(
                transactionMessage(),
                new CreateOrderTransactionContext(order)
        );

        assertEquals(RocketMQLocalTransactionState.ROLLBACK, state);
    }

    @Test
    void shouldReturnUnknownWhenLocalResultCannotBeConfirmed() {
        doThrow(new IllegalStateException("database connection interrupted"))
                .when(localTransactionService).createOrder(order);

        RocketMQLocalTransactionState state = listener.executeLocalTransaction(
                transactionMessage(),
                new CreateOrderTransactionContext(order)
        );

        assertEquals(RocketMQLocalTransactionState.UNKNOWN, state);
    }

    @Test
    void shouldCommitDuringBrokerCheckWhenMatchingOrderExists() {
        when(localTransactionService.isCommitted(order.orderNo(), order.eventId()))
                .thenReturn(true);

        assertEquals(
                RocketMQLocalTransactionState.COMMIT,
                listener.checkLocalTransaction(transactionMessage())
        );
    }

    @Test
    void shouldRollbackDuringBrokerCheckWhenOrderDoesNotExist() {
        when(localTransactionService.isCommitted(order.orderNo(), order.eventId()))
                .thenReturn(false);

        assertEquals(
                RocketMQLocalTransactionState.ROLLBACK,
                listener.checkLocalTransaction(transactionMessage())
        );
    }

    @Test
    void shouldReturnUnknownWhenBrokerCheckCannotQueryDatabase() {
        when(localTransactionService.isCommitted(order.orderNo(), order.eventId()))
                .thenThrow(new IllegalStateException("database unavailable"));

        assertEquals(
                RocketMQLocalTransactionState.UNKNOWN,
                listener.checkLocalTransaction(transactionMessage())
        );
    }

    private Message<String> transactionMessage() {
        return MessageBuilder.withPayload("half-message")
                .setHeader(OrderTransactionHeaders.ORDER_NO, order.orderNo())
                .setHeader(OrderTransactionHeaders.EVENT_ID, order.eventId())
                .build();
    }

    private PurchaseOrder unsavedOrder() {
        Instant now = Instant.parse("2026-08-10T01:00:00Z");
        return new PurchaseOrder(
                null,
                "TX-ORD-1001",
                "event-1001",
                1001L,
                2,
                OrderStatus.CREATED,
                now,
                now
        );
    }
}
