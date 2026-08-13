package com.example.rocketmq;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 依赖真实 MySQL 与 RocketMQ 的 Spring Boot 上下文启动集成测试。
 *
 * <p>此测试不验证业务功能，只验证 Spring 容器能否成功创建。
 * 如果依赖冲突或配置有严重错误，测试通常会在加载上下文时失败。</p>
 */
@SpringBootTest
@Tag("integration")
class RocketMqLearningApplicationIT {

    /**
     * 方法体可以为空，因为测试目标是触发并验证 Spring 上下文加载。
     */
    @Test
    void contextLoads() {
    }
}
