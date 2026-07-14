package com.erhernandez.kafka.exception;

public class TemporaryProcessingException extends RuntimeException {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public TemporaryProcessingException(String message) {
        super(message);
    }

}