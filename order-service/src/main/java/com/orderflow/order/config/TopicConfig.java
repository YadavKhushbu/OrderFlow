package com.orderflow.order.config;

import com.orderflow.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Creates the topics for local development and tests.
 *
 * <p>Declared in one service rather than in all three, because topics are shared
 * infrastructure and three services racing to create the same topic with
 * different settings is a configuration drift waiting to happen. In a real
 * deployment none of this belongs in application code at all: partition counts
 * and replication factors are capacity decisions, provisioned by Terraform or an
 * operator, not by whichever service happens to boot first.
 */
@Configuration
public class TopicConfig {

    @Bean
    public NewTopic inventoryCommandsTopic(@Value("${orderflow.topic-partitions:3}") int partitions) {
        return TopicBuilder.name(Topics.INVENTORY_COMMANDS).partitions(partitions).replicas(1).build();
    }

    @Bean
    public NewTopic paymentCommandsTopic(@Value("${orderflow.topic-partitions:3}") int partitions) {
        return TopicBuilder.name(Topics.PAYMENT_COMMANDS).partitions(partitions).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryEventsTopic(@Value("${orderflow.topic-partitions:3}") int partitions) {
        return TopicBuilder.name(Topics.INVENTORY_EVENTS).partitions(partitions).replicas(1).build();
    }

    @Bean
    public NewTopic paymentEventsTopic(@Value("${orderflow.topic-partitions:3}") int partitions) {
        return TopicBuilder.name(Topics.PAYMENT_EVENTS).partitions(partitions).replicas(1).build();
    }

    @Bean
    public NewTopic orderEventsTopic(@Value("${orderflow.topic-partitions:3}") int partitions) {
        return TopicBuilder.name(Topics.ORDER_EVENTS).partitions(partitions).replicas(1).build();
    }

    /** Single partition: dead letters are read by people, and ordering helps them. */
    @Bean
    public NewTopic deadLetterTopic() {
        return TopicBuilder.name(Topics.DEAD_LETTER).partitions(1).replicas(1).build();
    }
}
