package com.example.rocketmq.demo.basic.producer;

import com.example.rocketmq.infrastructure.messaging.RocketMqSendResultValidator;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.concurrent.CompletableFuture;

/**
 * RocketMQ 异步消息生产者。
 *
 * <p>与同步发送不同，调用 {@code asyncSend} 后，当前线程不等待 Broker 返回结果，
 * 最终结果会通过 {@link SendCallback} 回调通知。</p>
 */
@Service
public class AsyncMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(AsyncMessageProducer.class);

    private final RocketMQTemplate rocketMQTemplate;
    private final String destination;

    /**
     * 使用构造器注入，依赖明确，也便于以后编写单元测试。
     *
     * @param rocketMQTemplate Starter 提供的高级发送模板
     * @param topic             消息主题
     * @param tag               消息标签；本步骤沿用 sync 标签，因为同步/异步是发送方式，
     *                          不应该被当成业务分类
     */
    public AsyncMessageProducer(
            RocketMQTemplate rocketMQTemplate,
            @Value("${demo.rocketmq.topic}") String topic,
            @Value("${demo.rocketmq.sync-tag}") String tag) {
        this.rocketMQTemplate = rocketMQTemplate;
        this.destination = topic + ":" + tag;
    }

    /**
     * 异步发送一条普通字符串消息。
     *
     * <p>RocketMQ 原生使用回调返回结果。这里转换为 {@link CompletableFuture}，
     * 让上层业务可以使用 {@code thenApply}、{@code whenComplete} 等方式继续编排，
     * 同时避免把 RocketMQ 的回调结构扩散到业务层。</p>
     *
     * @param messageBody 消息正文
     * @return 代表本次发送最终结果的 Future；发送失败时以异常状态完成
     */
    public CompletableFuture<SendResult> send(String messageBody) {
        if (!StringUtils.hasText(messageBody)) {
            throw new IllegalArgumentException("消息内容不能为空");
        }

        CompletableFuture<SendResult> future = new CompletableFuture<>();

        try {
            rocketMQTemplate.asyncSend(destination, messageBody, new SendCallback() {
                /**
                 * Broker 确认发送结果后执行成功回调。
                 * 注意：发送成功只说明消息已到达 Broker，不代表 Consumer 已完成消费。
                 */
                @Override
                public void onSuccess(SendResult sendResult) {
                    try {
                        SendResult confirmedResult = RocketMqSendResultValidator.requireSendOk(
                                destination,
                                sendResult
                        );
                        log.info(
                                "RocketMQ 异步发送完成，destination={}, status={}, msgId={}, queueId={}, queueOffset={}",
                                destination,
                                confirmedResult.getSendStatus(),
                                confirmedResult.getMsgId(),
                                confirmedResult.getMessageQueue().getQueueId(),
                                confirmedResult.getQueueOffset()
                        );
                        future.complete(confirmedResult);
                    } catch (RuntimeException exception) {
                        // 回调被调用不等于业务成功；非 SEND_OK 必须让 Future 以异常状态完成。
                        log.error("RocketMQ 异步发送未获得明确成功回执，destination={}", destination, exception);
                        future.completeExceptionally(exception);
                    }
                }

                /**
                 * 网络异常、Broker 不可用或发送超时等问题会进入失败回调。
                 * 不能只记录日志后吞掉异常，否则业务层会误以为消息发送成功。
                 */
                @Override
                public void onException(Throwable throwable) {
                    log.error(
                            "RocketMQ 异步发送失败，destination={}, bodyLength={}",
                            destination,
                            messageBody.length(),
                            throwable
                    );
                    future.completeExceptionally(throwable);
                }
            });
        } catch (RuntimeException exception) {
            // asyncSend 在提交异步任务之前也可能同步失败，例如消息转换或客户端配置异常。
            log.error(
                    "RocketMQ 异步发送任务提交失败，destination={}, bodyLength={}",
                    destination,
                    messageBody.length(),
                    exception
            );
            future.completeExceptionally(exception);
        }

        return future;
    }
}
