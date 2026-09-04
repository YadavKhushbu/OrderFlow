package com.orderflow.messaging;

import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Scheduling and distributed locking for the outbox relay.
 *
 * <p>Every service running the relay needs exactly this, so it lives beside the
 * relay rather than being copied into each application.
 */
@Configuration
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "PT1M")
public class OutboxSchedulingConfig {

    /**
     * The lock lives in Postgres, which every service here already depends on.
     * Introducing a second datastore purely to hold one lock row would add a
     * failure mode and buy nothing.
     */
    @Bean
    public LockProvider lockProvider(JdbcTemplate jdbcTemplate) {
        return new JdbcTemplateLockProvider(
                JdbcTemplateLockProvider.Configuration.builder()
                        .withJdbcTemplate(jdbcTemplate)
                        .withTableName("shedlock")
                        // Database time rather than application time: clock skew
                        // between instances would otherwise let two of them think
                        // the lock had expired at the same moment.
                        .usingDbTime()
                        .build());
    }
}
