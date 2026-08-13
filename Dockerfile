# 当前构建直接使用本机 Maven 生成的可执行 JAR，避免额外下载 Maven 构建镜像。
# 打包命令：mvn -DskipTests package
FROM eclipse-temurin:17-jre
WORKDIR /app

# 使用非 root 用户运行应用，降低容器进程被利用后的权限范围。
RUN groupadd --system spring && useradd --system --gid spring spring
COPY --chown=spring:spring target/rocketmq-learning-*.jar app.jar

USER spring:spring
EXPOSE 8080

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-Dfile.encoding=UTF-8", "-jar", "/app/app.jar"]
