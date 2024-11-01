package com.programmingtechie.order_service.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderResquest {
    private List<OrderLineItemsDto> orderLineItemsDtoList;
}
