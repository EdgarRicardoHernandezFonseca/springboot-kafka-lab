package com.erhernandez.kafka.config.consumer;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

import com.erhernandez.kafka.avro.Notification;

@Configuration
public class NotificationConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Notification>
    notificationKafkaListenerFactory(
    		@Qualifier("notificationConsumerFactory")
    		ConsumerFactory<String, Notification> notificationConsumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, Notification> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(notificationConsumerFactory);

        factory.setConcurrency(2);

        factory.getContainerProperties()
                .setAckMode(ContainerProperties.AckMode.MANUAL);

        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

}