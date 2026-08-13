# RocketMQ Learning 项目结构

本项目使用“按业务模块优先、模块内部再分层”的结构，而不是把全项目所有 Controller、Service、
Repository 分别堆进三个大目录。随着订单和库存代码增长，修改一个业务时需要查看的文件仍集中在同一模块。

```text
com.example.rocketmq
├─ demo.basic                 第一、二阶段的字符串消息教学示例
│  ├─ producer
│  └─ consumer
├─ order                      订单业务模块
│  ├─ api                     REST 接口、请求校验与统一错误响应
│  ├─ domain                  纯 Java 领域对象与状态
│  ├─ service                 应用用例、事务编排与对外端口
│  ├─ repository              MySQL 持久化适配器
│  ├─ message                 消息契约与 Producer 适配器
│  ├─ consumer                订单相关 Consumer 适配器
│  └─ transaction             RocketMQ 事务消息监听器
├─ inventory                  库存业务模块
│  ├─ api                     只读库存查询接口
│  ├─ domain
│  ├─ service
│  ├─ repository
│  ├─ exception
│  └─ consumer
├─ infrastructure.messaging  跨模块消息可靠性、日志和消费记录基础设施
└─ infrastructure.web        统一 HTTP 错误契约与异常翻译
```

## 依赖方向

```text
Consumer / Producer / Transaction Listener（外部适配层）
                         ↓
                    Service（应用层）
                         ↓
             Domain + Repository 抽象/持久化层
```

约束：

1. `domain` 必须保持纯 Java，不依赖 Spring、RocketMQ、JDBC 或 MySQL。
2. `service` 可以使用 Spring 事务，但不能直接使用 RocketMQ 或 `JdbcTemplate`。
3. RocketMQ 类型只能出现在消息适配器、Consumer、事务监听器和公共消息基础设施中。
4. Repository 负责 SQL；Consumer 不直接执行 SQL；Service 不拼接 Topic 和 Tag。
5. `demo.basic` 只保存教学示例，正式订单和库存代码不能反向依赖它。

## 测试命名

- `*Test`：不依赖外部 MySQL/RocketMQ，普通 `mvn test` 自动运行。
- `*IT`：集成测试，需要在 IDEA 中显式运行，并提供对应环境变量与基础设施。

项目根目录的 `infrastructure/` 保存本地 MySQL 初始化脚本和 RocketMQ 启动配置；下载包、运行数据、
日志及项目内 Maven 缓存均已加入 `.gitignore`，不应提交到代码仓库。
