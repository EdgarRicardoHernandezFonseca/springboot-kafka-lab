package com.erhernandez.kafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlerConfig {

    @Bean
    public DefaultErrorHandler errorHandler() {

        FixedBackOff backOff =
                new FixedBackOff(2000L, 3);

        return new DefaultErrorHandler(backOff);

    }

}