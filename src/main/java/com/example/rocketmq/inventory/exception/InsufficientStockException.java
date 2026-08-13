package com.example.rocketmq.inventory.exception;

/**
 * 可用库存小于本次请求数量。
 */
public class InsufficientStockException extends RuntimeException {

    private final long productId;
    private final int requestedQuantity;
    private final int availableStock;

    public InsufficientStockException(
            long productId,
            int requestedQuantity,
            int availableStock) {
        super(String.format(
                "库存不足，productId=%d, requested=%d, available=%d",
                productId,
                requestedQuantity,
                availableStock
        ));
        this.productId = productId;
        this.requestedQuantity = requestedQuantity;
        this.availableStock = availableStock;
    }

    public long getProductId() {
        return productId;
    }

    public int getRequestedQuantity() {
        return requestedQuantity;
    }

    public int getAvailableStock() {
        return availableStock;
    }
}
