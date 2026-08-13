package com.example.rocketmq.inventory.service;

import com.example.rocketmq.inventory.domain.Inventory;
import com.example.rocketmq.inventory.exception.InsufficientStockException;
import com.example.rocketmq.inventory.exception.InventoryNotFoundException;
import com.example.rocketmq.inventory.repository.InventoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * 库存业务服务。
 *
 * <p>下一步 RocketMQ Consumer 只负责接收和解析消息，真正的扣减规则放在这里，
 * 这样同一业务能力也能被其他入口复用。</p>
 */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final InventoryRepository inventoryRepository;

    public InventoryService(InventoryRepository inventoryRepository) {
        this.inventoryRepository = inventoryRepository;
    }

    /**
     * 在本地数据库事务中扣减库存。
     */
    @Transactional
    public void deductStock(Long productId, Integer quantity) {
        if (productId == null || productId <= 0) {
            throw new IllegalArgumentException("productId 必须大于 0");
        }
        if (quantity == null || quantity <= 0) {
            throw new IllegalArgumentException("quantity 必须大于 0");
        }

        int affectedRows = inventoryRepository.deductStock(productId, quantity);
        if (affectedRows == 1) {
            log.info("库存扣减成功，productId={}, quantity={}", productId, quantity);
            return;
        }

        /*
         * UPDATE 返回 0 同时可能表示“不存在”和“不足”。这里只在失败路径补一次查询，
         * 成功路径不额外 SELECT，从而兼顾错误语义与正常吞吐量。
         */
        Optional<Inventory> currentInventory = inventoryRepository.findByProductId(productId);
        if (currentInventory.isEmpty()) {
            throw new InventoryNotFoundException(productId);
        }

        throw new InsufficientStockException(
                productId,
                quantity,
                currentInventory.orElseThrow().availableStock()
        );
    }

    /**
     * 查询商品当前库存，用于 API 验证异步扣减结果。
     *
     * <p>消息消费是异步的：创建订单接口成功不等于库存已在同一毫秒完成扣减，
     * 调用方可稍后查询这个接口观察最终一致性。</p>
     */
    public Inventory findInventory(Long productId) {
        if (productId == null || productId <= 0) {
            throw new IllegalArgumentException("productId 必须大于 0");
        }
        return inventoryRepository.findByProductId(productId)
                .orElseThrow(() -> new InventoryNotFoundException(productId));
    }
}
