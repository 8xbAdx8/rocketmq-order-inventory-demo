package com.example.rocketmq.order.transaction;

import com.example.rocketmq.infrastructure.messaging.MessageLogContext;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.messaging.Message;

/**
 * “创建订单”事务消息监听器。
 *
 * <p>{@link RocketMQTransactionListener} 会把该监听器绑定到默认的 {@code rocketMQTemplate}。
 * Producer 发送半消息成功后，Starter 会回调 {@link #executeLocalTransaction(Message, Object)}；
 * 如果 Broker 长时间没收到明确结果，则回调 {@link #checkLocalTransaction(Message)}。</p>
 */
@RocketMQTransactionListener(rocketMQTemplateBeanName = "rocketMQTemplate")
public class OrderCreateTransactionListener implements RocketMQLocalTransactionListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreateTransactionListener.class);

    private final OrderLocalTransactionService localTransactionService;

    public OrderCreateTransactionListener(OrderLocalTransactionService localTransactionService) {
        this.localTransactionService = localTransactionService;
    }

    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message message, Object arg) {
        if (!(arg instanceof CreateOrderTransactionContext context)) {
            log.error("订单事务消息缺少合法的本地事务上下文，拒绝提交半消息");
            return RocketMQLocalTransactionState.ROLLBACK;
        }

        try (MessageLogContext ignored = MessageLogContext.forSend(
                "ORDER_CREATED_TRANSACTION_LOCAL",
                context.order().eventId(),
                context.order().orderNo())) {
            try {
                localTransactionService.createOrder(context.order());
                log.info("订单本地事务提交成功");
                return RocketMQLocalTransactionState.COMMIT;
            } catch (DuplicateKeyException | IllegalArgumentException exception) {
                // 唯一键冲突或参数非法属于明确的业务失败，消息不应再投递给消费者。
                log.warn("订单本地事务明确失败，回滚半消息：{}", exception.getMessage());
                return RocketMQLocalTransactionState.ROLLBACK;
            } catch (RuntimeException exception) {
                /*
                 * 网络中断、数据库连接闪断等异常下，客户端未必能确认事务最终状态。
                 * 返回 UNKNOWN 比武断地回滚更安全，Broker 稍后会依据数据库事实发起回查。
                 */
                log.error("订单本地事务结果暂时无法确认，等待 Broker 回查", exception);
                return RocketMQLocalTransactionState.UNKNOWN;
            }
        }
    }

    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message message) {
        String orderNo = headerAsString(message, OrderTransactionHeaders.ORDER_NO);
        String eventId = headerAsString(message, OrderTransactionHeaders.EVENT_ID);
        if (orderNo == null || eventId == null) {
            log.error("事务回查消息缺少 orderNo 或 eventId，无法关联本地订单，执行回滚");
            return RocketMQLocalTransactionState.ROLLBACK;
        }

        try (MessageLogContext ignored = MessageLogContext.forSend(
                "ORDER_CREATED_TRANSACTION_CHECK",
                eventId,
                orderNo)) {
            try {
                boolean committed = localTransactionService.isCommitted(orderNo, eventId);
                log.info("订单事务回查完成，committed={}", committed);
                return committed
                        ? RocketMQLocalTransactionState.COMMIT
                        : RocketMQLocalTransactionState.ROLLBACK;
            } catch (RuntimeException exception) {
                // 查询数据库本身失败时不能据此认定本地事务失败，让 Broker 后续继续回查。
                log.error("订单事务回查暂时失败", exception);
                return RocketMQLocalTransactionState.UNKNOWN;
            }
        }
    }

    private String headerAsString(Message message, String headerName) {
        Object value = message.getHeaders().get(headerName);
        if (value == null || value.toString().isBlank()) {
            return null;
        }
        return value.toString();
    }

}
