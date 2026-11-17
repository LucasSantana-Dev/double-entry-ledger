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
USER 10001:10001
EXPOSE 8080
# Phase 5 (ADR 0027), measured with tools/footprint: SerialGC (what the JVM picks on its own
# under 1,792 MB, written down; same peak memory as G1 and about 8% less CPU for the same load),
# 60% of the container limit for the heap, and metaspace capped well above the 116 MB the
# application uses, so the rest of the limit is left for metaspace, code cache and threads.
ENV JAVA_TOOL_OPTIONS="-XX:+UseSerialGC -XX:MaxRAMPercentage=60 -XX:MaxMetaspaceSize=192m"
ENTRYPOINT ["java", "-jar", "application.jar"]
