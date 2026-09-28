package io.github.lindseyz1205.videopipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class VideoPipelineApplication {

    public static void main(String[] args) {
        SpringApplication.run(VideoPipelineApplication.class, args);
    }
}
