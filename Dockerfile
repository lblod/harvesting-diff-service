FROM maven:3.8-openjdk-18 as builder
LABEL maintainer="info@redpencil.io"

WORKDIR /app

COPY pom.xml .

COPY .mvn .mvn

COPY settings.xml settings.xml

RUN mvn verify --fail-never

COPY ./src ./src

RUN mvn package -DskipTests

FROM openjdk:18

WORKDIR /app

COPY --from=builder /app/target/harvesting-validator.jar ./app.jar

ENTRYPOINT [ "java", "-jar","/app/app.jar"]
