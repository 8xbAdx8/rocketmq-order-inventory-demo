package com.example.rocketmq.infrastructure.messaging;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;

/** 对所有同步、异步、顺序和事务发送结果执行同一套成功判定。 */
public final class RocketMqSendResultValidator {

    private RocketMqSendResultValidator() {
    }

    /**
     * 只有 {@link SendStatus#SEND_OK} 才表示 Broker 明确确认成功。
     * FLUSH_DISK_TIMEOUT 等状态虽然带有 SendResult，也不能被业务代码当成确定成功。
     */
    public static <T extends SendResult> T requireSendOk(String destination, T sendResult) {
        if (sendResult == null) {
            throw new RocketMqSendException(
                    destination,
                    null,
                    "RocketMQ 未返回发送结果，destination=" + destination
            );
        }
        if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
            throw new RocketMqSendException(
                    destination,
                    sendResult.getSendStatus(),
                    "RocketMQ 未明确确认发送成功，destination=" + destination
                            + ", status=" + sendResult.getSendStatus()
            );
        }
        return sendResult;
    }
}
