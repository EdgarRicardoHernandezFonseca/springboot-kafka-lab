package com.erhernandez.kafka.exception;

public class InvalidCustomerException extends NonRetryableBusinessException {

	public InvalidCustomerException(String message) {
		super(message);
	}
}