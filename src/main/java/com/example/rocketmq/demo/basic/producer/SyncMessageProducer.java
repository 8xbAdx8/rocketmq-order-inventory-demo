package com.example.rocketmq.demo.basic.producer;

import com.example.rocketmq.infrastructure.messaging.RocketMqSendResultValidator;
import org.apache.rocketmq.client.producer.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.apache.rocketmq.spring.core.RocketMQTemplate;

/**
 * 同步普通消息生产者。
 *
 * <p>这个类只负责“把一条已经准备好的字符串消息发往 RocketMQ”。
 * 它不负责创建订单、修改数据库或扣减库存，避免中间件代码与业务逻辑混在一起。</p>
 */
@Service
public class SyncMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(SyncMessageProducer.class);

    /**
     * RocketMQ Spring 封装的高层发送模板。
     * 它帮我们处理底层 Producer 的创建、启动、序列化和资源释放。
     */
    private final RocketMQTemplate rocketMQTemplate;

    /**
     * 完整发送目的地，格式为 {@code Topic:Tag}。
     */
    private final String destination;

    /**
     * 使用构造器注入，使依赖关系明确，也便于后续单元测试。
     *
     * @param rocketMQTemplate Starter 自动配置的 RocketMQ 发送模板
     * @param topic            application.yml 中配置的 Topic
     * @param tag              application.yml 中配置的 Tag
     */
    public SyncMessageProducer(
            RocketMQTemplate rocketMQTemplate,
            @Value("${demo.rocketmq.topic}") String topic,
            @Value("${demo.rocketmq.sync-tag}") String tag) {
        this.rocketMQTemplate = rocketMQTemplate;
        this.destination = topic + ":" + tag;
    }

    /**
     * 同步发送一条普通字符串消息。
     *
     * <p>“同步”表示当前线程会等待 Broker 返回发送结果。
     * 成功返回 {@link SendResult}；超时、无路由或网络异常时会抛出异常，
     * 本阶段先让异常向上传播，企业级重试与落库补偿放在第五阶段实现。</p>
     *
     * @param messageBody 要发送的消息内容，不允许为空白字符串
     * @return Broker 返回的发送结果
     */
    public SendResult send(String messageBody) {
        if (!StringUtils.hasText(messageBody)) {
            throw new IllegalArgumentException("messageBody must not be blank");
        }

        // syncSend 在收到 Broker 结果前会阻塞当前线程。
        SendResult sendResult = RocketMqSendResultValidator.requireSendOk(
                destination,
                rocketMQTemplate.syncSend(destination, messageBody)
        );

        // 实际项目应优先记录 msgId、状态、队列与位点，避免直接打印可能含隐私的消息正文。
        log.info(
                "RocketMQ sync send completed: destination={}, status={}, msgId={}, queue={}, queueOffset={}",
                destination,
                sendResult.getSendStatus(),
                sendResult.getMsgId(),
                sendResult.getMessageQueue(),
                sendResult.getQueueOffset());

        return sendResult;
    }
}
