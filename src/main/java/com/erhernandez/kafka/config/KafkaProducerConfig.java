package com.erhernandez.kafka.config;

import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaProducerConfig {

    // Actualmente Spring Boot autoconfigura el Producer
    // utilizando spring.kafka.producer.*
    //
    // Esta clase queda preparada para futuras
    // configuraciones:
    //
    // - ProducerFactory
    // - KafkaTemplate
    // - Transactional Producer
    // - Custom Partitioner
    // - Interceptors

}