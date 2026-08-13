package com.example.rocketmq.inventory.api;

import com.example.rocketmq.inventory.domain.Inventory;
import com.example.rocketmq.inventory.exception.InventoryNotFoundException;
import com.example.rocketmq.inventory.service.InventoryService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 库存查询接口的 MVC 切片测试。 */
@WebMvcTest(InventoryController.class)
class InventoryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private InventoryService inventoryService;

    @Test
    void shouldReturnCurrentInventory() throws Exception {
        Instant now = Instant.parse("2026-08-11T08:00:00Z");
        when(inventoryService.findInventory(1001L))
                .thenReturn(new Inventory(1001L, 98, 1L, now, now));

        mockMvc.perform(get("/api/inventories/1001"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.productId").value(1001))
                .andExpect(jsonPath("$.availableStock").value(98))
                .andExpect(jsonPath("$.version").value(1));
    }

    @Test
    void shouldReturnNotFoundWhenInventoryDoesNotExist() throws Exception {
        when(inventoryService.findInventory(2001L))
                .thenThrow(new InventoryNotFoundException(2001L));

        mockMvc.perform(get("/api/inventories/2001"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message").value("商品库存不存在，productId=2001"));
    }
}
