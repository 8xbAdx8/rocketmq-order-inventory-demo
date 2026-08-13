package com.example.rocketmq.architecture;

import com.example.rocketmq.RocketMqLearningApplication;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * 不引入额外框架的轻量架构测试。
 *
 * <p>通过扫描编译后 class 常量池，防止后续开发者把 RocketMQ/JDBC 类型重新引入领域层和应用层。</p>
 */
class ArchitectureBoundaryTest {

    @Test
    void domainMustRemainFrameworkIndependent() throws Exception {
        assertPackagesDoNotContain(
                List.of("com.example.rocketmq.order.domain", "com.example.rocketmq.inventory.domain"),
                List.of("org/springframework", "org/apache/rocketmq", "org/springframework/jdbc", "com/mysql")
        );
    }

    @Test
    void servicesMustNotDependDirectlyOnRocketMqOrJdbc() throws Exception {
        assertPackagesDoNotContain(
                List.of("com.example.rocketmq.order.service", "com.example.rocketmq.inventory.service"),
                List.of("org/apache/rocketmq", "org/springframework/jdbc", "com/mysql")
        );
    }

    private void assertPackagesDoNotContain(
            List<String> packageNames,
            List<String> forbiddenInternalNames) throws Exception {
        Path classRoot = Paths.get(
                RocketMqLearningApplication.class
                        .getProtectionDomain()
                        .getCodeSource()
                        .getLocation()
                        .toURI()
        );
        List<String> violations = new ArrayList<>();

        for (String packageName : packageNames) {
            Path packageRoot = classRoot.resolve(packageName.replace('.', '/'));
            if (!Files.exists(packageRoot)) {
                fail("未找到待检查的编译包：" + packageRoot);
            }
            scanPackage(packageRoot, classRoot, forbiddenInternalNames, violations);
        }

        if (!violations.isEmpty()) {
            fail("发现架构越层依赖：\n" + String.join("\n", violations));
        }
    }

    private void scanPackage(
            Path packageRoot,
            Path classRoot,
            List<String> forbiddenInternalNames,
            List<String> violations) throws IOException {
        try (var paths = Files.walk(packageRoot)) {
            for (Path classFile : paths.filter(path -> path.toString().endsWith(".class")).toList()) {
                // class 常量池中的类名使用 ASCII，ISO-8859-1 可一字节不丢失地检查这些标识。
                String bytecode = new String(Files.readAllBytes(classFile), StandardCharsets.ISO_8859_1);
                for (String forbidden : forbiddenInternalNames) {
                    if (bytecode.contains(forbidden)) {
                        violations.add(classRoot.relativize(classFile) + " -> " + forbidden);
                    }
                }
            }
        }
    }
}
