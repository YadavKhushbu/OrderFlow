package com.orderflow.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * The scan paths are spelled out because the outbox and deduplication machinery
 * lives in the shared {@code com.orderflow.messaging} module, outside this
 * application's own package. Spring Boot's defaults only look below the class
 * annotated with {@code @SpringBootApplication}, so without these the shared
 * entities and repositories would simply not exist at runtime.
 */
@SpringBootApplication(scanBasePackages = {"com.orderflow.payment", "com.orderflow.messaging"})
@EntityScan({"com.orderflow.payment", "com.orderflow.messaging"})
@EnableJpaRepositories({"com.orderflow.payment", "com.orderflow.messaging"})
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
