FROM openjdk:23-slim

RUN apt-get update && apt-get install -y procps && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mkdir -p /app/config

COPY build/libs/kafka-consumer-*.jar /app/app.jar

ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    KAFKA_CONSUMER_GROUP=functional-group \
    KAFKA_TOPIC=json-topic \
    APP_NAME=kafka-consumer-app

ENTRYPOINT ["java", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "--enable-preview", \
    "-jar", "/app/app.jar"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD pgrep -f "java.*app.jar" || exit 1