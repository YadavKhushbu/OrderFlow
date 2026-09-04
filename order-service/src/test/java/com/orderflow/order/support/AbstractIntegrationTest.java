package com.orderflow.order.support;

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
 * Base for tests that run against a real Postgres and a real Kafka.
 *
 * <p>Embedded Kafka would be faster, but the behaviour under test here is
 * partitioning, consumer groups, offset commits and redelivery. Those are the
 * things an in-memory stand-in approximates rather than implements, so a test
 * that passed against one would prove very little about the thing that actually
 * runs in production.
 */
@AbstractIntegrationTest.IntegrationTest
public abstract class AbstractIntegrationTest {

    /**
     * Applied to concrete test classes rather than inherited from here.
     *
     * <p>JUnit resolves conditions against the class it is about to run, and a
     * skip condition that quietly fails to inherit does not produce a skipped
     * test — it produces a whole class erroring out on any machine without
     * Docker, which looks exactly like a real failure.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Documented
    @Inherited
    @SpringBootTest
    @ActiveProfiles("test")
    @EnabledIf("com.orderflow.order.support.AbstractIntegrationTest#dockerIsAvailable")
    public @interface IntegrationTest {
    }

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("orderflow_orders")
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

    /**
     * Started lazily, never from a static initialiser: JUnit must load this class
     * to evaluate the skip condition, and a static block would run first and blow
     * up before the skip could apply.
     */
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

    protected static String bootstrapServers() {
        ensureStarted();
        return KAFKA.getBootstrapServers();
    }
}
