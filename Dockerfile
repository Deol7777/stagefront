# One Dockerfile, four images.
#
# Every service is a module of the same Maven reactor and shares the `contracts`
# module, so building one in isolation still needs the parent POM and its
# siblings. Rather than four near-identical Dockerfiles that each have to know
# that, this takes the module name as a build argument:
#
#   docker build --build-arg SERVICE=order-service -t ticketing/order-service .
#
# (docker-compose passes it for each service — see the `apps` profile.)
#
# Build context is the REPO ROOT, not the service directory, for the same reason.

# ---- Stage 1: build ---------------------------------------------------------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Copy ONLY the POMs first, then pre-fetch dependencies.
#
# This is the layer-caching trick that makes rebuilds fast. Docker caches each
# layer keyed on the files it copied; source changes every commit, but the
# dependency set changes rarely. Splitting them means editing a .java file
# reuses the cached dependency layer instead of re-downloading the internet.
# Copy the sources first and you invalidate that cache on every single build.
COPY pom.xml .
COPY contracts/pom.xml contracts/
COPY services/order-service/pom.xml services/order-service/
COPY services/inventory-service/pom.xml services/inventory-service/
COPY services/payment-service/pom.xml services/payment-service/
COPY services/notification-service/pom.xml services/notification-service/

# -B batch mode (no ANSI progress spam in build logs).
# Failure here is non-fatal: go-offline can't always resolve every plugin, and
# the real build below will fetch whatever is missing.
RUN mvn -B -q dependency:go-offline -DskipTests || true

# Now the sources — this is the layer that actually changes.
COPY contracts/src contracts/src
COPY services/order-service/src services/order-service/src
COPY services/inventory-service/src services/inventory-service/src
COPY services/payment-service/src services/payment-service/src
COPY services/notification-service/src services/notification-service/src

ARG SERVICE
# -pl <module> -am: build this module AND the modules it depends on (contracts),
# skipping the other three services. -am is what makes per-service images
# possible without building the whole world each time.
RUN test -n "$SERVICE" || (echo "SERVICE build-arg is required" && exit 1) \
 && mvn -B -q -pl services/${SERVICE} -am -DskipTests package \
 && cp services/${SERVICE}/target/*.jar /build/app.jar

# ---- Stage 2: runtime -------------------------------------------------------
# JRE, not JDK: the compiler and build tooling have no business in a runtime
# image — smaller surface, less to patch.
FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app

# Run as a non-root user. If the process is ever compromised, it does not own
# the container.
RUN useradd --system --uid 10001 --create-home appuser
USER appuser

COPY --from=build --chown=appuser:appuser /build/app.jar app.jar

# Spring Boot reads this; container-aware memory defaults come free with JDK 21,
# so no manual -Xmx guessing against the container limit.
ENV JAVA_OPTS=""

EXPOSE 8080
# exec form via sh so $JAVA_OPTS expands, and `exec` so the JVM becomes PID 1 —
# without it the shell is PID 1 and the JVM never receives SIGTERM, so the
# container is SIGKILLed after the grace period instead of shutting down
# cleanly. For a Kafka consumer that means no orderly offset commit.
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
