package com.example.rocketmq.infrastructure.messaging;

/**
 * 当前教学项目统一使用的 RocketMQ 消费可靠性参数。
 *
 * <p>注解中的数字必须是编译期常量，不能直接读取 application.yml，因此集中放在这里，
 * 避免四个 Consumer 各自写一个含义不明的数字。</p>
 */
public final class RocketMqReliabilityPolicy {

    /** 首次消费失败后最多再消费 3 次，仍失败则进入该 Consumer Group 的 DLQ。 */
    public static final int MAX_RECONSUME_TIMES = 3;

    private RocketMqReliabilityPolicy() {
    }
}
