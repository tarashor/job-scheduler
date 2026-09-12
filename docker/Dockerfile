# Multi-stage build for distributed microservices
FROM eclipse-temurin:25-jdk-alpine AS builder
WORKDIR /app
COPY . .
RUN ./gradlew installDist --no-daemon

# -----------------------------------------------
# Target: API Gateway Microservice
# -----------------------------------------------
FROM eclipse-temurin:25-jre-alpine AS api
WORKDIR /app
COPY --from=builder /app/scheduler-api/build/install/scheduler-api /app
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["/app/bin/scheduler-api"]

# -----------------------------------------------
# Target: Master Coordinator Microservice
# -----------------------------------------------
FROM eclipse-temurin:25-jre-alpine AS coordinator
WORKDIR /app
COPY --from=builder /app/scheduler-coordinator/build/install/scheduler-coordinator /app
ENTRYPOINT ["/app/bin/scheduler-coordinator"]

# -----------------------------------------------
# Target: Worker Daemon Microservice
# -----------------------------------------------
FROM eclipse-temurin:25-jre-alpine AS worker
WORKDIR /app
COPY --from=builder /app/scheduler-worker/build/install/scheduler-worker /app
ENTRYPOINT ["/app/bin/scheduler-worker"]
