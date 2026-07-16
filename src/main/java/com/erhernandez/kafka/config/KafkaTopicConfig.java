package com.erhernandez.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic ordersTopic() {

    	return TopicBuilder
                .name("orders")
                .partitions(3)
                .replicas(1)
                .build();
    }

    
    @Bean
    public NewTopic ordersDltTopic(){

        return TopicBuilder
                .name("orders-dlt")
                .partitions(3)
                .replicas(1)
                .build();

    }
    
    @Bean
    public NewTopic notificationsDltTopic(){

        return TopicBuilder
                .name("notifications-dlt")
                .partitions(3)
                .replicas(1)
                .build();

    }
    
    @Bean
    public NewTopic notificationsTopic() {
        return TopicBuilder.name("notifications")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
