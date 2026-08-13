-- V3：记录每个 Consumer Group 已成功处理的业务事件，用于消费幂等。
-- 同一事件可以被不同业务 Consumer Group 各处理一次，但同一 Group 不能重复处理。

CREATE TABLE `message_consume_record`
(
    `id`             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '数据库主键',
    `consumer_group` VARCHAR(128)    NOT NULL COMMENT '消费组名称',
    `event_id`       VARCHAR(128)    NOT NULL COMMENT '生产者生成的业务事件唯一标识',
    `message_type`   VARCHAR(64)     NOT NULL COMMENT '消息业务类型',
    `business_key`   VARCHAR(128)    NOT NULL COMMENT '业务主键，本案例为订单号',
    `consumed_at`    DATETIME(3)     NOT NULL COMMENT 'UTC 消费时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_message_consume_group_event` (`consumer_group`, `event_id`),
    KEY `idx_message_consume_business_key` (`business_key`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '消息成功消费记录表';
