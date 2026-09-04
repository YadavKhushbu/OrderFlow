package com.orderflow.messaging;

import com.orderflow.events.MessageHeaders;
import com.orderflow.events.Topics;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.nio.charset.StandardCharsets;

/**
 * Retry-then-dead-letter behaviour, identical in every service.
 *
 * <h2>Why a failing message must eventually be abandoned</h2>
 *
 * <p>Kafka has no per-message redelivery and no selective acknowledgement. A
 * consumer that keeps failing on record N cannot move past it, so every message
 * behind it on that partition stops moving too. Retrying forever therefore does
 * not make the system more reliable — it converts one broken message into a
 * stalled partition and a growing pile of unprocessed orders.
 *
 * <p>So attempts are bounded, and the message is then parked on the dead-letter
 * topic with enough context to triage it. Losing one order to manual
 * intervention is strictly better than silently freezing all of them.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                template,
                (record, exception) -> {
                    log.error("Dead-lettering record from {}-{} offset {} after retries were exhausted",
                            record.topic(), record.partition(), record.offset(), exception);

                    // Written onto the record so triage starts with the facts
                    // rather than with a hunt through logs for a stack trace.
                    record.headers().add(MessageHeaders.DLT_ORIGINAL_TOPIC,
                            record.topic().getBytes(StandardCharsets.UTF_8));
                    record.headers().add(MessageHeaders.DLT_REASON,
                            String.valueOf(exception.getMessage()).getBytes(StandardCharsets.UTF_8));

                    return new TopicPartition(Topics.DEAD_LETTER, 0);
                });

        // Exponential, because most transient failures — a database failover, a
        // dependency restarting — clear within seconds, and retrying a
        // recovering dependency at full speed is how a blip becomes an outage.
        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxElapsedTime(30_000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(UnparseableMessageException.class);
        return handler;
    }

    @Bean
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        // Delegates to Boot's own configuration first, so everything under
        // spring.kafka.* still applies and only the error handler is replaced.
        // The raw cast is unavoidable: the configurer is typed <Object, Object>
        // while listeners here are typed <String, String>, and the erased types
        // are identical at runtime.
        configurer.configure((ConcurrentKafkaListenerContainerFactory) factory, consumerFactory);

        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
