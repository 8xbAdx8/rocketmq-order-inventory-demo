-- V4：为可直接运行的作品集演示准备一条库存数据。
-- 企业生产环境通常由商品/库存后台维护库存，不会在结构迁移里写业务数据；
-- 本项目保留该种子数据是为了让首次 docker compose up 后能立即验证异步扣减。

INSERT INTO `inventory` (`product_id`, `available_stock`, `version`)
VALUES (1001, 100, 0)
ON DUPLICATE KEY UPDATE `product_id` = VALUES(`product_id`);
