-- V2：记录订单生命周期消息的真实消费顺序。
-- 该表既用于教学验收，也模拟企业项目中的事件处理审计记录。

CREATE TABLE `order_lifecycle_event_log`
(
    `id`          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '消费顺序主键',
    `event_id`    VARCHAR(64)     NOT NULL COMMENT '消息事件唯一标识',
    `order_no`    VARCHAR(64)     NOT NULL COMMENT '业务订单号',
    `event_type`  VARCHAR(32)     NOT NULL COMMENT 'CREATED、PAID、SHIPPED',
    `occurred_at` DATETIME(3)     NOT NULL COMMENT '业务事件发生时间（UTC）',
    `consumed_at` DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '消费落库时间（UTC）',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_order_lifecycle_event_id` (`event_id`),
    KEY `idx_order_lifecycle_order_sequence` (`order_no`, `id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '订单生命周期消息消费日志';
