package com.erhernandez.kafka.exception;

public class RetryableBusinessException extends BusinessException {

	public RetryableBusinessException(String message) {
		super(message);
	}
}