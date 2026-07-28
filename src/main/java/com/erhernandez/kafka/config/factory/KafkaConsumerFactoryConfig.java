package com.erhernandez.kafka.config.factory;

import java.util.Map;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import com.erhernandez.kafka.avro.Notification;
import com.erhernandez.kafka.avro.OrderCreated;

@Configuration
public class KafkaConsumerFactoryConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaConsumerFactoryConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * Obtiene toda la configuración definida en application.yml.
     */
    private Map<String, Object> consumerProperties() {
        return kafkaProperties.buildConsumerProperties();
    }

    /**
     * Método reutilizable para crear ConsumerFactory tipados.
     */
    private <T> ConsumerFactory<String, T> buildConsumerFactory() {

        return new DefaultKafkaConsumerFactory<>(
                consumerProperties()
        );
    }

    @Bean("orderConsumerFactory")
    public ConsumerFactory<String, OrderCreated> orderConsumerFactory() {
        return buildConsumerFactory();
    }

    @Bean("notificationConsumerFactory")
    public ConsumerFactory<String, Notification> notificationConsumerFactory() {
        return buildConsumerFactory();
    }

    @Bean("orderDeadLetterConsumerFactory")
    public ConsumerFactory<String, OrderCreated> orderDeadLetterConsumerFactory() {
        return buildConsumerFactory();
    }

    @Bean("notificationDeadLetterConsumerFactory")
    public ConsumerFactory<String, Notification> notificationDeadLetterConsumerFactory() {
        return buildConsumerFactory();
    }

}