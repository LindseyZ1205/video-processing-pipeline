package io.github.lindseyz1205.videopipeline.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Title and description of the generated OpenAPI document, served at {@code /v3/api-docs} and rendered at
 * {@code /swagger-ui.html}.
 */
@Configuration(proxyBeanMethods = false)
public class OpenApiConfiguration {

    @Bean
    public OpenAPI pipelineOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Video processing pipeline API")
                .version("0.1.0")
                .description("Request a presigned upload URL, PUT the file straight to S3, then follow the "
                        + "transcription job until it completes.")
                .license(new License()
                        .name("MIT")
                        .url("https://github.com/LindseyZ1205/video-processing-pipeline/blob/main/LICENSE")));
    }
}
