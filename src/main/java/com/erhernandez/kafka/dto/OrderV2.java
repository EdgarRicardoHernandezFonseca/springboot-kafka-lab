package com.erhernandez.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderV2 {

	private Long orderId;

    private String customerName;
    
    private String priority;

    private String product;
    
    private Integer quantity;
    
    private Double price;
    
    private String createdAt;
}
