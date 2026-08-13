-- V1：建立订单与库存的最小持久化模型。
-- Flyway 成功执行后会记录版本和校验和；已经执行过的 V1 不会在下次启动时重复执行。

CREATE TABLE `purchase_order`
(
    `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '数据库主键',
    `order_no`   VARCHAR(64)     NOT NULL COMMENT '对外业务订单号',
    `event_id`   VARCHAR(64)     NOT NULL COMMENT '订单创建事件唯一标识',
    `product_id` BIGINT UNSIGNED NOT NULL COMMENT '商品标识',
    `quantity`   INT UNSIGNED    NOT NULL COMMENT '购买数量',
    `status`     VARCHAR(32)     NOT NULL COMMENT '订单状态：CREATED、PAID、CANCELLED、SHIPPED',
    `created_at` DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'UTC 创建时间',
    `updated_at` DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'UTC 更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_purchase_order_order_no` (`order_no`),
    UNIQUE KEY `uk_purchase_order_event_id` (`event_id`),
    KEY `idx_purchase_order_product_id` (`product_id`),
    CONSTRAINT `chk_purchase_order_quantity` CHECK (`quantity` > 0)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '订单表';

CREATE TABLE `inventory`
(
    `product_id`      BIGINT UNSIGNED NOT NULL COMMENT '商品标识，同时作为主键',
    `available_stock` INT UNSIGNED    NOT NULL COMMENT '当前可用库存',
    `version`         BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '乐观锁版本号',
    `created_at`      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'UTC 创建时间',
    `updated_at`      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'UTC 更新时间',
    PRIMARY KEY (`product_id`),
    CONSTRAINT `chk_inventory_available_stock` CHECK (`available_stock` >= 0)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '商品库存表';
