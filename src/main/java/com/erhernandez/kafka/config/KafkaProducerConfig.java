package com.erhernandez.kafka.config;

import com.erhernandez.kafka.avro.Notification;
import com.erhernandez.kafka.avro.OrderCreated;
import com.erhernandez.kafka.dto.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    /*
     * ============================================================
     * Order producer - JSON
     * ============================================================
     */

    @Bean
    public ProducerFactory<String, Order> orderProducerFactory() {

        Map<String, Object> config = new HashMap<>();

        config.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        config.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        config.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class
        );

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Order> orderKafkaTemplate() {

        return new KafkaTemplate<>(
                orderProducerFactory()
        );
    }

    /*
     * ============================================================
     * OrderCreated producer - Avro
     * ============================================================
     */

    @Bean
    public ProducerFactory<String, OrderCreated> orderCreatedProducerFactory() {

        Map<String, Object> config = avroProducerProperties();

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, OrderCreated> orderCreatedKafkaTemplate() {

        return new KafkaTemplate<>(
                orderCreatedProducerFactory()
        );
    }

    /*
     * ============================================================
     * Notification producer - Avro
     * ============================================================
     */

    @Bean
    public ProducerFactory<String, Notification> notificationProducerFactory() {

        Map<String, Object> config = avroProducerProperties();

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Notification> notificationKafkaTemplate() {

        return new KafkaTemplate<>(
                notificationProducerFactory()
        );
    }

    /*
     * ============================================================
     * Common Avro producer configuration
     * ============================================================
     */

    private Map<String, Object> avroProducerProperties() {

        Map<String, Object> config = new HashMap<>();

        config.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        config.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        config.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class
        );

        config.put(
                "schema.registry.url",
                schemaRegistryUrl
        );

        config.put(
                "auto.register.schemas",
                true
        );

        return config;
    }
}