package com.erhernandez.kafka.config.consumer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

import com.erhernandez.kafka.avro.Notification;

@Configuration
public class NotificationDeadLetterConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Notification>
    notificationDeadLetterKafkaListenerFactory(

            ConsumerFactory<String, Notification> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, Notification> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        factory.setConcurrency(1);

        factory.getContainerProperties()
                .setAckMode(ContainerProperties.AckMode.MANUAL);

        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

}