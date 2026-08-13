package com.example.rocketmq.order.service;

import com.example.rocketmq.order.message.OrderLifecycleEventType;
import com.example.rocketmq.order.message.OrderLifecycleMessage;
import com.example.rocketmq.order.repository.OrderLifecycleEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * 校验并记录订单生命周期消息的消费顺序。
 */
@Service
public class OrderLifecycleEventService {

    private static final Logger log = LoggerFactory.getLogger(OrderLifecycleEventService.class);

    private final OrderLifecycleEventRepository repository;

    public OrderLifecycleEventService(OrderLifecycleEventRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public void recordInOrder(OrderLifecycleMessage message) {
        Optional<OrderLifecycleEventType> latest = repository.findLatestEventType(
                message.orderNo()
        );
        if (!isExpectedNext(latest, message.eventType())) {
            throw new IllegalStateException(
                    "订单生命周期消息乱序，orderNo=" + message.orderNo()
                            + ", previous=" + latest.map(Enum::name).orElse("NONE")
                            + ", current=" + message.eventType()
            );
        }

        repository.append(message);
        log.info(
                "订单生命周期事件按顺序处理成功，orderNo={}, eventType={}, previous={}",
                message.orderNo(),
                message.eventType(),
                latest.map(Enum::name).orElse("NONE")
        );
    }

    private boolean isExpectedNext(
            Optional<OrderLifecycleEventType> latest,
            OrderLifecycleEventType current) {
        return switch (current) {
            case CREATED -> latest.isEmpty();
            case PAID -> latest.orElse(null) == OrderLifecycleEventType.CREATED;
            case SHIPPED -> latest.orElse(null) == OrderLifecycleEventType.PAID;
        };
    }
}
