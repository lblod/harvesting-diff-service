FROM maven:3.9-eclipse-temurin-21 AS builder
LABEL maintainer="info@redpencil.io"

WORKDIR /app

COPY pom.xml .

RUN mvn verify --fail-never

COPY ./src ./src

RUN mvn package -DskipTests

FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=builder /app/target/harvesting-diff.jar ./app.jar

ENTRYPOINT ["java", "-XX:+CompactStrings","-jar", "/app/app.jar"]
