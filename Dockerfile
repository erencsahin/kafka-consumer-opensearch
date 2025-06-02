FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /build

COPY . .
RUN mvn clean package -pl kafka-consumer-opensearch -am -DskipTests -q

FROM eclipse-temurin:17-jdk-jammy
WORKDIR /app

COPY --from=builder /build/kafka-consumer-opensearch/target/kafka-consumer-opensearch-*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","app.jar"]