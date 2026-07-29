package com.erhernandez.kafka.exception;

public class NonRetryableBusinessException extends BusinessException {

	public NonRetryableBusinessException(String message) {
		super(message);
	}

}