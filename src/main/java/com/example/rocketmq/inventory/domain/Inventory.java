package com.example.rocketmq.inventory.domain;

import java.time.Instant;
import java.util.Objects;

/**
 * 商品库存领域对象。
 *
 * @param productId      商品 ID，同时也是库存表主键
 * @param availableStock 当前可用库存，不能小于 0
 * @param version        库存变更版本号，每次成功扣减后加 1
 * @param createdAt      UTC 创建时间
 * @param updatedAt      UTC 更新时间
 */
public record Inventory(
        Long productId,
        Integer availableStock,
        Long version,
        Instant createdAt,
        Instant updatedAt
) {

    public Inventory {
        Objects.requireNonNull(productId, "productId 不能为空");
        if (productId <= 0) {
            throw new IllegalArgumentException("productId 必须大于 0");
        }

        Objects.requireNonNull(availableStock, "availableStock 不能为空");
        if (availableStock < 0) {
            throw new IllegalArgumentException("availableStock 不能小于 0");
        }

        Objects.requireNonNull(version, "version 不能为空");
        if (version < 0) {
            throw new IllegalArgumentException("version 不能小于 0");
        }

        Objects.requireNonNull(createdAt, "createdAt 不能为空");
        Objects.requireNonNull(updatedAt, "updatedAt 不能为空");
    }
}
