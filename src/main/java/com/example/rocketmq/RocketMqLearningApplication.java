package com.example.rocketmq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 项目的启动入口。
 *
 * <p>目前这个类只负责启动 Spring Boot，不包含任何 RocketMQ 或业务逻辑。
 * 这样可以先验证 Java、Maven 与 Spring Boot 基础环境是否正常。</p>
 */
@SpringBootApplication
public class RocketMqLearningApplication {

    /**
     * Java 程序入口。
     *
     * @param args IDEA 或命令行传入的启动参数
     */
    public static void main(String[] args) {
        /*
         * SpringApplication.run 会创建 Spring 容器、执行自动配置，
         * 并完成应用启动。返回值是本次启动创建的应用上下文。
         */
        SpringApplication.run(RocketMqLearningApplication.class, args);
    }
}
