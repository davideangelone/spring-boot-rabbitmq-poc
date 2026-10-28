package it.davideangelone.rabbitmq;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class RabbitMqExchangesApplication {

    private static final String SWAGGER_UI_URL = "http://localhost:8080/swagger-ui.html";
    private static final String RABBITMQ_CONSOLE = "http://localhost:15672";

    public static void main(String[] args) {
        SpringApplication.run(RabbitMqExchangesApplication.class, args);
        log.info("Application started");
        log.info("Swagger UI       : {}", SWAGGER_UI_URL);
        log.info("RabbitMQ Console : {}", RABBITMQ_CONSOLE);
    }
}
