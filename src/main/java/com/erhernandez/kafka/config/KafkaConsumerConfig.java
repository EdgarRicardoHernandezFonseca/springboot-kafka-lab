package com.erhernandez.kafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

@Configuration
public class KafkaConsumerConfig {

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String,Object>
	kafkaListenerContainerFactory(

	        ConsumerFactory<String,Object> consumerFactory,

	        DefaultErrorHandler defaultErrorHandler){

	    ConcurrentKafkaListenerContainerFactory<String,Object> factory =
	            new ConcurrentKafkaListenerContainerFactory<>();

	    factory.setConsumerFactory(consumerFactory);

	    factory.getContainerProperties()
	            .setAckMode(ContainerProperties.AckMode.MANUAL);

	    factory.setCommonErrorHandler(defaultErrorHandler);

	    return factory;

	}

}