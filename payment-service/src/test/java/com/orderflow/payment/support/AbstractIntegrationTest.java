package com.orderflow.payment.support;

import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.lang.annotation.*;

/**
 * Real Postgres and real Kafka, for the same reason as everywhere else in this
 * project: the behaviour under test is constraint enforcement and message
 * delivery, neither of which an in-memory substitute implements faithfully.
 */
public abstract class AbstractIntegrationTest {

    /** Applied to concrete classes; JUnit resolves conditions on the class it runs. */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Documented
    @Inherited
    @SpringBootTest
    @ActiveProfiles("test")
    @EnabledIf("com.orderflow.payment.support.AbstractIntegrationTest#dockerIsAvailable")
    public @interface IntegrationTest {
    }

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("orderflow_payment")
                    .withUsername("orderflow")
                    .withPassword("orderflow");

    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    private static final boolean DOCKER_AVAILABLE = probeDocker();
    private static boolean started;

    public static boolean dockerIsAvailable() {
        return DOCKER_AVAILABLE;
    }

    private static boolean probeDocker() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            return false;
        }
    }

    /** Lazy, never a static block: the skip condition must be evaluated first. */
    private static synchronized void ensureStarted() {
        if (!started) {
            POSTGRES.start();
            KAFKA.start();
            started = true;
        }
    }

    @DynamicPropertySource
    static void wireContainers(DynamicPropertyRegistry registry) {
        ensureStarted();
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }
}
