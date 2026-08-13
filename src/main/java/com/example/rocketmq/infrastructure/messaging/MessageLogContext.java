package com.example.rocketmq.infrastructure.messaging;

import org.slf4j.MDC;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 一次消息发送或消费的结构化日志上下文。
 *
 * <p>MDC 数据只属于当前线程。RocketMQ Consumer 使用线程池，如果处理结束后不清理，
 * 下一条消息可能错误继承上一条消息的 eventId。本类通过 try-with-resources 自动恢复原值。</p>
 */
public final class MessageLogContext implements AutoCloseable {

    public static final String MESSAGE_TYPE = "messageType";
    public static final String EVENT_ID = "eventId";
    public static final String BUSINESS_KEY = "businessKey";
    public static final String CONSUMER_GROUP = "consumerGroup";

    private final Map<String, String> previousValues = new LinkedHashMap<>();
    private boolean closed;

    private MessageLogContext(Map<String, String> values) {
        values.forEach((key, value) -> {
            previousValues.put(key, MDC.get(key));
            if (value == null || value.isBlank()) {
                MDC.remove(key);
            } else {
                MDC.put(key, value);
            }
        });
    }

    /** 为发送链路创建上下文；发送端没有 Consumer Group。 */
    public static MessageLogContext forSend(
            String messageType,
            String eventId,
            String businessKey) {
        return open(messageType, eventId, businessKey, null);
    }

    /** 为消费链路创建上下文。 */
    public static MessageLogContext forConsume(
            String messageType,
            String eventId,
            String businessKey,
            String consumerGroup) {
        return open(messageType, eventId, businessKey, consumerGroup);
    }

    private static MessageLogContext open(
            String messageType,
            String eventId,
            String businessKey,
            String consumerGroup) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put(MESSAGE_TYPE, messageType);
        values.put(EVENT_ID, eventId);
        values.put(BUSINESS_KEY, businessKey);
        values.put(CONSUMER_GROUP, consumerGroup);
        return new MessageLogContext(values);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        previousValues.forEach((key, previousValue) -> {
            if (previousValue == null) {
                MDC.remove(key);
            } else {
                MDC.put(key, previousValue);
            }
        });
        closed = true;
    }
}
