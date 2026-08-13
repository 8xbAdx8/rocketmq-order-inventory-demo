package com.example.rocketmq.inventory.api;

import com.example.rocketmq.inventory.service.InventoryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 库存只提供查询入口；真正的扣减入口是 RocketMQ Consumer，
 * 从接口层面体现订单服务与库存服务之间没有同步扣减调用。
 */
@RestController
@RequestMapping("/api/inventories")
public class InventoryController {

    private final InventoryService inventoryService;

    public InventoryController(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @GetMapping("/{productId}")
    public InventoryResponse findInventory(@PathVariable Long productId) {
        return InventoryResponse.from(inventoryService.findInventory(productId));
    }
}
