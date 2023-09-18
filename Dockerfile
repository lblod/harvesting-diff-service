FROM maven:3.8-openjdk-18 as builder
LABEL maintainer="info@redpencil.io"

WORKDIR /app

COPY pom.xml .

RUN mvn verify --fail-never

COPY ./src ./src

RUN mvn package -DskipTests

FROM eclipse-temurin:18-jre

WORKDIR /app

COPY --from=builder /app/target/harvesting-diff.jar ./app.jar

ENTRYPOINT [ "java", "-jar","/app/app.jar"]
