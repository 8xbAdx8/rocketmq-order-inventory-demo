package com.example.rocketmq.infrastructure.messaging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** 验证消息线程处理结束后 MDC 不会污染线程池中的下一条消息。 */
class MessageLogContextTest {

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    void shouldExposeTraceFieldsAndRestorePreviousContextAfterClose() {
        MDC.put(MessageLogContext.EVENT_ID, "outer-event");

        try (MessageLogContext ignored = MessageLogContext.forConsume(
                "ORDER_CREATED",
                "event-1001",
                "ORD-1001",
                "inventory-group")) {
            assertEquals("ORDER_CREATED", MDC.get(MessageLogContext.MESSAGE_TYPE));
            assertEquals("event-1001", MDC.get(MessageLogContext.EVENT_ID));
            assertEquals("ORD-1001", MDC.get(MessageLogContext.BUSINESS_KEY));
            assertEquals("inventory-group", MDC.get(MessageLogContext.CONSUMER_GROUP));
        }

        assertNull(MDC.get(MessageLogContext.MESSAGE_TYPE));
        assertEquals("outer-event", MDC.get(MessageLogContext.EVENT_ID));
        assertNull(MDC.get(MessageLogContext.BUSINESS_KEY));
        assertNull(MDC.get(MessageLogContext.CONSUMER_GROUP));
    }
}
