package com.erhernandez.kafka.exception;

public abstract class BusinessException extends RuntimeException {

	public BusinessException(String message) {
		super(message);
	}
}