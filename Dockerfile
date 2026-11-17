# syntax=docker/dockerfile:1
# Multi-stage image (ADR 0024): JDK 25 builds the jar, JRE 25 runs it as a non-root user.

FROM eclipse-temurin:25.0.5_8-jdk-noble AS build
WORKDIR /build
# Dependencies first: this layer is rebuilt only when the POM or the wrapper changes.
COPY .mvn/wrapper/maven-wrapper.properties .mvn/wrapper/
COPY mvnw pom.xml ./
RUN ./mvnw -B -q dependency:go-offline
COPY src/ src/
# Tests run in the CI verify job, not in the image build.
RUN ./mvnw -B -q -DskipTests package \
    && cp target/double-entry-ledger-*.jar application.jar \
    && java -Djarmode=tools -jar application.jar extract --layers --destination extracted

FROM eclipse-temurin:25.0.5_8-jre-noble
RUN groupadd --system --gid 10001 ledger \
    && useradd --system --uid 10001 --gid ledger --no-create-home ledger
WORKDIR /app
# Spring Boot layers, least to most often changed.
COPY --from=build /build/extracted/dependencies/ ./
COPY --from=build /build/extracted/spring-boot-loader/ ./
COPY --from=build /build/extracted/snapshot-dependencies/ ./
COPY --from=build /build/extracted/application/ ./
# Phase 5 (ADR 0027), measured with tools/footprint: SerialGC (what the JVM picks on its own
# under 1,792 MB, written down; same peak memory as G1 and about 8% less CPU for the same load),
# 60% of the container limit for the heap, and a metaspace cap well above the 116 MB the
# application uses. Set before the training run below, which must use the same GC.
ENV JAVA_TOOL_OPTIONS="-XX:+UseSerialGC -XX:MaxRAMPercentage=60 -XX:MaxMetaspaceSize=192m"
# Phase 5 (ADR 0028): JVM AOT cache (JEP 483, one-step creation from JEP 514). A training start
# of the application loads and links its classes and the JVM writes them to app.aot on exit;
# the runtime maps the file instead of loading those classes again. It runs in this stage
# because the cache only works with the exact JVM and class path that created it.
# The build has no PostgreSQL or Kafka, so the training stops right after the context refresh
# (spring.context.exit=onRefresh), before anything is started, and the beans that would contact
# them at refresh are told not to: Flyway off, Hibernate without JDBC metadata (dialect given),
# KafkaAdmin without topic creation, and a datasource URL that is never connected to (Hikari
# opens connections lazily). The classes of the skipped work (Flyway, the JDBC driver's
# connection path, the Kafka admin client) are not in the cache and load normally at runtime.
RUN java -XX:AOTCacheOutput=app.aot -Dspring.context.exit=onRefresh -jar application.jar         --spring.datasource.url=jdbc:postgresql://training.invalid:5432/none         --spring.flyway.enabled=false         --spring.jpa.hibernate.ddl-auto=none         --spring.jpa.properties.hibernate.boot.allow_jdbc_metadata_access=false         --spring.jpa.database-platform=org.hibernate.dialect.PostgreSQLDialect         --spring.kafka.admin.auto-create=false
USER 10001:10001
EXPOSE 8080
# -XX:AOTCache: the JVM carries on without the cache, with a warning, if it does not match.
ENTRYPOINT ["java", "-XX:AOTCache=app.aot", "-jar", "application.jar"]
