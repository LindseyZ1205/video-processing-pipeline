# syntax=docker/dockerfile:1

# Build with the Gradle wrapper inside the image, so the host doesn't need Java.
FROM eclipse-temurin:17-jdk AS build
WORKDIR /src
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
COPY src ./src
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon bootJar \
    && cp build/libs/video-processing-pipeline-*[0-9].jar /application.jar
# Split the fat jar into layers, dependencies first. A code change then rebuilds only the small application layer.
RUN java -Djarmode=tools -jar /application.jar extract --layers --destination /extracted

FROM eclipse-temurin:17-jre
LABEL org.opencontainers.image.source="https://github.com/LindseyZ1205/video-processing-pipeline" \
      org.opencontainers.image.description="Video upload and transcription pipeline on S3, SQS and DynamoDB" \
      org.opencontainers.image.licenses="MIT"
RUN groupadd --system app && useradd --system --gid app app
WORKDIR /app
COPY --from=build /extracted/dependencies/ ./
COPY --from=build /extracted/spring-boot-loader/ ./
COPY --from=build /extracted/snapshot-dependencies/ ./
COPY --from=build /extracted/application/ ./
USER app
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "application.jar"]
