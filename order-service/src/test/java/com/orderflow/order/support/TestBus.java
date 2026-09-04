package com.orderflow.order.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.events.MessageHeaders;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * Stands in for the other services during an order-service test.
 *
 * <p>Lets a test read the commands the saga emitted and reply with the events a
 * real payment or inventory service would have sent. Driving the saga through
 * the bus rather than by calling its methods is what makes these tests cover
 * serialisation, headers, partitioning and the outbox relay as well as the state
 * machine — all the places a distributed flow actually breaks.
 */
public class TestBus implements AutoCloseable {

    private final String bootstrapServers;
    private final ObjectMapper objectMapper;
    private final KafkaProducer<String, String> producer;
    private final List<KafkaConsumer<String, String>> consumers = new ArrayList<>();

    public TestBus(String bootstrapServers, ObjectMapper objectMapper) {
        this.bootstrapServers = bootstrapServers;
        this.objectMapper = objectMapper;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Subscribes before the action that produces the messages.
     *
     * <p>Must be called first: a consumer created afterwards with
     * {@code auto.offset.reset=earliest} would still work here, but subscribing
     * up front avoids depending on that and on partition-assignment timing.
     */
    public Listener listenTo(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));
        // Forces partition assignment now, so nothing produced after this call
        // can be missed.
        consumer.poll(Duration.ofMillis(500));
        consumers.add(consumer);
        return new Listener(consumer);
    }

    /** Publishes a reply as the payment or inventory service would have. */
    public void publish(String topic, UUID sagaId, String messageType, Object payload) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic, sagaId.toString(), objectMapper.writeValueAsString(payload));

            record.headers().add(new RecordHeader(MessageHeaders.MESSAGE_ID,
                    UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader(MessageHeaders.SAGA_ID,
                    sagaId.toString().getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader(MessageHeaders.MESSAGE_TYPE,
                    messageType.getBytes(StandardCharsets.UTF_8)));

            producer.send(record).get();
        } catch (Exception e) {
            throw new IllegalStateException("Test could not publish to " + topic, e);
        }
    }

    /** Publishes with an explicit message id, so a redelivery can be simulated exactly. */
    public void publishWithMessageId(String topic, UUID sagaId, UUID messageId,
                                     String messageType, Object payload) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic, sagaId.toString(), objectMapper.writeValueAsString(payload));
            record.headers().add(new RecordHeader(MessageHeaders.MESSAGE_ID,
                    messageId.toString().getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader(MessageHeaders.SAGA_ID,
                    sagaId.toString().getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader(MessageHeaders.MESSAGE_TYPE,
                    messageType.getBytes(StandardCharsets.UTF_8)));
            producer.send(record).get();
        } catch (Exception e) {
            throw new IllegalStateException("Test could not publish to " + topic, e);
        }
    }

    @Override
    public void close() {
        producer.close();
        consumers.forEach(KafkaConsumer::close);
    }

    /** A subscription a test can drain and assert against. */
    public class Listener {

        private final KafkaConsumer<String, String> consumer;
        private final List<ConsumerRecord<String, String>> collected = new ArrayList<>();

        private Listener(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }

        /**
         * Waits for a message of the given type, returning its deserialised payload.
         *
         * @throws AssertionError if none arrives in time, naming what did arrive
         */
        public <T> T awaitMessage(String messageType, Class<T> type, Duration timeout) {
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : drainOnce()) {
                    if (messageType.equals(headerValue(record, MessageHeaders.MESSAGE_TYPE))) {
                        try {
                            return objectMapper.readValue(record.value(), type);
                        } catch (Exception e) {
                            throw new AssertionError("Message of type " + messageType
                                    + " could not be deserialised: " + record.value(), e);
                        }
                    }
                }
            }
            throw new AssertionError("Timed out waiting for a " + messageType
                    + " message. Saw instead: " + seenTypes());
        }

        public List<ConsumerRecord<String, String>> drainOnce() {
            ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(250));
            List<ConsumerRecord<String, String>> batch = new ArrayList<>();
            polled.forEach(batch::add);
            collected.addAll(batch);
            return batch;
        }

        /** Every message seen so far, for assertions about what was <em>not</em> sent. */
        public List<String> seenTypes() {
            return collected.stream()
                    .map(r -> headerValue(r, MessageHeaders.MESSAGE_TYPE))
                    .toList();
        }

        public long countOf(String messageType) {
            return seenTypes().stream().filter(messageType::equals).count();
        }

        private String headerValue(ConsumerRecord<String, String> record, String name) {
            var header = record.headers().lastHeader(name);
            return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
        }
    }
}
