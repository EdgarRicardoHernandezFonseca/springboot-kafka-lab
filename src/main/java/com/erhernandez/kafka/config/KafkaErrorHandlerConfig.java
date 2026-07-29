package com.erhernandez.kafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import com.erhernandez.kafka.exception.NonRetryableBusinessException;
import com.erhernandez.kafka.exception.RetryableBusinessException;

@Configuration
public class KafkaErrorHandlerConfig {

	@Bean
	public DefaultErrorHandler defaultErrorHandler() {
		
		System.out.println("******** MI HANDLER ********");

	    FixedBackOff backOff = new FixedBackOff(2000L, 3);

	    DefaultErrorHandler handler =
	            new DefaultErrorHandler(backOff);

	    handler.addRetryableExceptions(
	            RetryableBusinessException.class);

	    handler.addNotRetryableExceptions(
	            NonRetryableBusinessException.class);

	    return handler;
	}
}