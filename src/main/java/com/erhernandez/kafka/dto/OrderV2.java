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
}
