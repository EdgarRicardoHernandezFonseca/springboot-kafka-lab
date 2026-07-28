package com.erhernandez.kafka.config.consumer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

import com.erhernandez.kafka.avro.OrderCreated;

@Configuration
public class OrderDeadLetterConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreated>
    orderDeadLetterKafkaListenerFactory(

            ConsumerFactory<String, OrderCreated> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, OrderCreated> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        factory.setConcurrency(1);

        factory.getContainerProperties()
                .setAckMode(ContainerProperties.AckMode.MANUAL);

        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

}