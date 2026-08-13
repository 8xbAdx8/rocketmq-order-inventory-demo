package com.example.rocketmq.inventory.service;

import com.example.rocketmq.inventory.domain.Inventory;
import com.example.rocketmq.inventory.exception.InsufficientStockException;
import com.example.rocketmq.inventory.exception.InventoryNotFoundException;
import com.example.rocketmq.inventory.repository.InventoryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * 库存业务规则的快速单元测试，不连接 MySQL。
 */
class InventoryServiceTest {

    private InventoryRepository inventoryRepository;
    private InventoryService inventoryService;

    @BeforeEach
    void setUp() {
        inventoryRepository = mock(InventoryRepository.class);
        inventoryService = new InventoryService(inventoryRepository);
    }

    @Test
    void shouldFinishWhenConditionalUpdateSucceeds() {
        when(inventoryRepository.deductStock(1001L, 2)).thenReturn(1);

        inventoryService.deductStock(1001L, 2);

        verify(inventoryRepository).deductStock(1001L, 2);
        // 成功路径不需要再查询库存，减少一次数据库访问。
        verify(inventoryRepository, never()).findByProductId(1001L);
    }

    @Test
    void shouldThrowInsufficientStockWhenInventoryExists() {
        when(inventoryRepository.deductStock(1001L, 5)).thenReturn(0);
        when(inventoryRepository.findByProductId(1001L))
                .thenReturn(Optional.of(inventory(1001L, 3)));

        InsufficientStockException exception = assertThrows(
                InsufficientStockException.class,
                () -> inventoryService.deductStock(1001L, 5)
        );

        assertEquals(1001L, exception.getProductId());
        assertEquals(5, exception.getRequestedQuantity());
        assertEquals(3, exception.getAvailableStock());
    }

    @Test
    void shouldThrowNotFoundWhenInventoryDoesNotExist() {
        when(inventoryRepository.deductStock(2001L, 1)).thenReturn(0);
        when(inventoryRepository.findByProductId(2001L)).thenReturn(Optional.empty());

        InventoryNotFoundException exception = assertThrows(
                InventoryNotFoundException.class,
                () -> inventoryService.deductStock(2001L, 1)
        );

        assertEquals(2001L, exception.getProductId());
    }

    @Test
    void shouldRejectInvalidQuantityBeforeAccessingRepository() {
        assertThrows(
                IllegalArgumentException.class,
                () -> inventoryService.deductStock(1001L, 0)
        );

        verifyNoInteractions(inventoryRepository);
    }

    @Test
    void shouldFindInventory() {
        Inventory expected = inventory(1001L, 100);
        when(inventoryRepository.findByProductId(1001L))
                .thenReturn(Optional.of(expected));

        Inventory result = inventoryService.findInventory(1001L);

        assertEquals(expected, result);
    }

    @Test
    void shouldReportMissingInventoryWhenQuerying() {
        when(inventoryRepository.findByProductId(2001L)).thenReturn(Optional.empty());

        InventoryNotFoundException exception = assertThrows(
                InventoryNotFoundException.class,
                () -> inventoryService.findInventory(2001L)
        );

        assertEquals(2001L, exception.getProductId());
    }

    private Inventory inventory(long productId, int availableStock) {
        Instant now = Instant.parse("2026-08-09T10:00:00Z");
        return new Inventory(productId, availableStock, 0L, now, now);
    }
}
