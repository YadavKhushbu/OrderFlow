# syntax=docker/dockerfile:1
# Built from the repository root: docker build -f order-service/Dockerfile .
# The whole multi-module project is needed because this service depends on the
# common-events and common-messaging modules.

FROM eclipse-temurin:17-jdk-alpine AS build
WORKDIR /build

# Poms first, so the dependency layer is cached and only re-resolved when a
# dependency actually changes rather than on every source edit.
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
COPY common-events/pom.xml common-events/
COPY common-messaging/pom.xml common-messaging/
COPY order-service/pom.xml order-service/
COPY payment-service/pom.xml payment-service/
COPY inventory-service/pom.xml inventory-service/
RUN chmod +x mvnw && ./mvnw -B -q dependency:go-offline -DskipTests || true

COPY common-events/src common-events/src
COPY common-messaging/src common-messaging/src
COPY order-service/src order-service/src

# Tests need Docker for Testcontainers, so they run in CI, not in an image build.
RUN ./mvnw -B -pl order-service -am package -DskipTests

FROM eclipse-temurin:17-jre-alpine AS runtime
WORKDIR /app
RUN addgroup -S app && adduser -S app -G app
COPY --from=build --chown=app:app /build/order-service/target/*.jar app.jar
USER app

# MaxRAMPercentage rather than a fixed -Xmx, so the JVM respects whatever the
# container limit turns out to be instead of a number baked in at build time.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
