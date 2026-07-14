package com.erhernandez.kafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import com.erhernandez.kafka.exception.InvalidOrderException;

@Configuration
public class KafkaErrorHandlerConfig {

	@Bean
	public DefaultErrorHandler defaultErrorHandler(
	        KafkaTemplate<Object,Object> kafkaTemplate){

	    DeadLetterPublishingRecoverer recoverer =
	            new DeadLetterPublishingRecoverer(kafkaTemplate);

	    FixedBackOff backOff =
	            new FixedBackOff(2000L,3);

	    DefaultErrorHandler errorHandler =
	            new DefaultErrorHandler(
	                    recoverer,
	                    backOff
	            );

	    errorHandler.addNotRetryableExceptions(

	            IllegalArgumentException.class,
	            NullPointerException.class,
	            InvalidOrderException.class

	    );

	    return errorHandler;
	}
}