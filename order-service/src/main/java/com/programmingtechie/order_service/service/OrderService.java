package com.programmingtechie.order_service.service;

import com.programmingtechie.order_service.dto.InventoryResponse;
import com.programmingtechie.order_service.dto.OrderLineItemsDto;
import com.programmingtechie.order_service.dto.OrderResquest;
import com.programmingtechie.order_service.event.OrderPlacedEvent;
import com.programmingtechie.order_service.model.Order;
import com.programmingtechie.order_service.model.OrderLineItems;
import com.programmingtechie.order_service.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Transactional
public class OrderService {

    private final OrderRepository orderRepository;
    private final WebClient.Builder webClientBuilder;
    private final KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate;

    public String placeOrder(OrderResquest orderResquest) {
        Order order = new Order();
        order.setOrderNumber(UUID.randomUUID().toString());

        List<OrderLineItems> orderLineItems = orderResquest.getOrderLineItemsDtoList()
                .stream()
                .map(this::mapToDto)
                .toList();

        order.setOrderLineItemsList(orderLineItems);

        List<String> skuCodes = order.getOrderLineItemsList().stream()
                .map(OrderLineItems::getSkuCode)
                .toList();

        // Gọi dịch vụ tồn kho để kiểm tra sự tồn tại của SKU
        InventoryResponse[] inventoryResponseArray = webClientBuilder.build().get()
                .uri("http://inventory-service/api/inventory",
                        uriBuilder -> uriBuilder.queryParam("skuCode", skuCodes).build())
                .retrieve()
                .bodyToMono(InventoryResponse[].class)
                .block();

        assert inventoryResponseArray != null;

        // Kiểm tra các SKU không có và hết hàng
        List<String> unavailableSkuCodes = skuCodes.stream()
                .filter(skuCode -> Arrays.stream(inventoryResponseArray)
                        .noneMatch(inventoryResponse -> inventoryResponse.getSkuCode().equals(skuCode))
                ).toList();

        List<String> outOfStockSkuCodes = skuCodes.stream()
                .filter(skuCode -> Arrays.stream(inventoryResponseArray)
                        .anyMatch(inventoryResponse -> inventoryResponse.getSkuCode().equals(skuCode) && !inventoryResponse.isInStock())
                ).toList();

        if (!unavailableSkuCodes.isEmpty()) {
            return "Invalid SKU codes: " + unavailableSkuCodes;
        }

        if (!outOfStockSkuCodes.isEmpty()) {
           return "Out of stock for SKU codes: " + outOfStockSkuCodes;
        }

        // Nếu tất cả sản phẩm có sẵn, lưu đơn hàng
        orderRepository.save(order);
        kafkaTemplate.send("notificationTopic", new OrderPlacedEvent(order.getOrderNumber()));
        return "Order Placed Successfully";
    }

    private OrderLineItems mapToDto(OrderLineItemsDto orderLineItemsDto) {
        OrderLineItems orderLineItems = new OrderLineItems();
        orderLineItems.setPrice(orderLineItemsDto.getPrice());
        orderLineItems.setQuantity(orderLineItemsDto.getQuantity());
        orderLineItems.setSkuCode(orderLineItemsDto.getSkuCode());
        return orderLineItems;
    }
}
