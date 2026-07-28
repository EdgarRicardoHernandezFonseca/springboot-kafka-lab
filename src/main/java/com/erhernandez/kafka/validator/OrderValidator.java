package com.erhernandez.kafka.validator;

import org.springframework.stereotype.Component;

import com.erhernandez.kafka.avro.OrderCreated;
import com.erhernandez.kafka.exception.InvalidCustomerException;
import com.erhernandez.kafka.exception.RetryableBusinessException;

@Component
public class OrderValidator {

    public void validate(OrderCreated order) {

        if (order.getOrderId() % 2 == 0) {
            throw new RetryableBusinessException(
                    "Retry test");
        }

        String customer =
                order.getCustomerName() == null
                        ? null
                        : order.getCustomerName().toString();

        if ("ERROR".equalsIgnoreCase(customer)) {
            throw new RetryableBusinessException(
                    "Temporary processing error");
        }

        if (customer == null || customer.isBlank()) {
            throw new InvalidCustomerException(
                    "Customer name is mandatory");
        }

    }

}