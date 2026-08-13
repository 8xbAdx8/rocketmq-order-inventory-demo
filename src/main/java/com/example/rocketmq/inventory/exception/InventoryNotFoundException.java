package com.example.rocketmq.inventory.exception;

/**
 * 商品尚未建立库存记录。
 */
public class InventoryNotFoundException extends RuntimeException {

    private final long productId;

    public InventoryNotFoundException(long productId) {
        super("商品库存不存在，productId=" + productId);
        this.productId = productId;
    }

    public long getProductId() {
        return productId;
    }
}
