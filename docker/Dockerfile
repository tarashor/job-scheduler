# Multi-stage build for distributed microservices
FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /app
COPY . .
RUN ./gradlew bootJar --no-daemon

# -----------------------------------------------
# Target: API Gateway Microservice
# -----------------------------------------------
FROM eclipse-temurin:21-jre-alpine AS api
WORKDIR /app
COPY --from=builder /app/scheduler-api/build/libs/scheduler-api-1.0-SNAPSHOT.jar /app/app.jar
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

# -----------------------------------------------
# Target: Master Coordinator Microservice
# -----------------------------------------------
FROM eclipse-temurin:21-jre-alpine AS coordinator
WORKDIR /app
COPY --from=builder /app/scheduler-coordinator/build/libs/scheduler-coordinator-1.0-SNAPSHOT.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

# -----------------------------------------------
# Target: Worker Daemon Microservice
# -----------------------------------------------
FROM eclipse-temurin:21-jre-alpine AS worker
WORKDIR /app
COPY --from=builder /app/scheduler-worker/build/libs/scheduler-worker-1.0-SNAPSHOT.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

