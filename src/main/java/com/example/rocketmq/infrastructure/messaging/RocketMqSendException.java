package com.example.rocketmq.infrastructure.messaging;

import org.apache.rocketmq.client.producer.SendStatus;

/**
 * Broker 没有明确确认消息写入成功时抛出的统一异常。
 *
 * <p>业务上层必须感知发送失败，随后才能选择返回失败、事务回滚、写补偿表或报警；
 * 不能只打印日志后继续返回成功。</p>
 */
public class RocketMqSendException extends RuntimeException {

    private final String destination;
    private final SendStatus sendStatus;

    public RocketMqSendException(String destination, SendStatus sendStatus, String message) {
        super(message);
        this.destination = destination;
        this.sendStatus = sendStatus;
    }

    public String getDestination() {
        return destination;
    }

    public SendStatus getSendStatus() {
        return sendStatus;
    }
}
