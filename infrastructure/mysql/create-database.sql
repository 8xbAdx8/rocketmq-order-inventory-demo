-- 该脚本只负责创建数据库，应由拥有 CREATE DATABASE 权限的管理员执行一次。
-- 应用表结构不放在这里，而是由 src/main/resources/db/migration 下的 Flyway 脚本管理。
CREATE DATABASE IF NOT EXISTS `rocketmq_learning`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;
