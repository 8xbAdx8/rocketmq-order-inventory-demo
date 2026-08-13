package com.example.rocketmq.infrastructure.web;

import java.time.Instant;
import java.util.Map;

/** 统一错误响应，避免每个业务 Controller 返回不同格式。 */
public record ApiError(
        Instant timestamp,
        int status,
        String error,
        String message,
        String path,
        Map<String, String> fieldErrors
) {
}
