package com.erhernandez.kafka.exception;

public class NonRetryableBusinessException extends RuntimeException {

    public NonRetryableBusinessException(String message) {
        super(message);
    }

}