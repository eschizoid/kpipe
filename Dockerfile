FROM openjdk:24-slim

RUN apt-get update && apt-get install -y procps && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mkdir -p /app/config

COPY app/build/libs/kpipe-*.jar /app/app.jar

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    KAFKA_CONSUMER_GROUP=kpipe-group \
    KAFKA_TOPIC=json-topic \
    APP_NAME=kpipe

ENTRYPOINT ["java", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "--enable-preview", \
    "-jar", \
    "/app/app.jar"]

#HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
#    CMD pgrep -f "java.*app.jar" || exit 1