package com.example.rocketmq.inventory.api;

import com.example.rocketmq.inventory.domain.Inventory;

import java.time.Instant;

/** 库存查询的稳定 HTTP 响应契约。 */
public record InventoryResponse(
        Long productId,
        Integer availableStock,
        Long version,
        Instant createdAt,
        Instant updatedAt
) {

    public static InventoryResponse from(Inventory inventory) {
        return new InventoryResponse(
                inventory.productId(),
                inventory.availableStock(),
                inventory.version(),
                inventory.createdAt(),
                inventory.updatedAt()
        );
    }
}
