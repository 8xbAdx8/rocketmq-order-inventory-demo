package com.example.rocketmq.inventory.service;

/** 幂等消费结果，便于日志和测试区分首次处理与重复跳过。 */
public enum MessageConsumeResult {
    PROCESSED,
    DUPLICATE_SKIPPED
}
