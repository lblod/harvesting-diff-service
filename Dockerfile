FROM maven:3.8-openjdk-21 as builder
LABEL maintainer="info@redpencil.io"

WORKDIR /app

COPY pom.xml .

RUN mvn verify --fail-never

COPY ./src ./src

RUN mvn package -DskipTests

FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=builder /app/target/harvesting-diff.jar ./app.jar

ENV JAVA_OPTS=""
ENTRYPOINT ["java", "-XX:+CompactStrings", "${JAVA_OPTS}","-jar", "/app/app.jar"]
