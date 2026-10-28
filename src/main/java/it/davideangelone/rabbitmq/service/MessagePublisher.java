package it.davideangelone.rabbitmq.service;

import java.util.Map;

import it.davideangelone.rabbitmq.api.PublishRequest;
import it.davideangelone.rabbitmq.api.PublishResponse;
import it.davideangelone.rabbitmq.config.RabbitTopology;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class MessagePublisher {

    private final RabbitTemplate rabbitTemplate;

    public MessagePublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public PublishResponse publishDirect(PublishRequest request) {
        String key = requireRoutingKey(request.routingKey(), "ad esempio [orders] oppure [billing]");
        log.info("Scrittura su direct exchange con routingKey={}", key);
        return publish(RabbitTopology.DIRECT_EXCHANGE, key, request, Map.of());
    }

    public PublishResponse publishFanout(PublishRequest request) {
        log.info("Scrittura su fanout exchange");
        // Fanout ignora la routing key; la stringa vuota rende esplicito il comportamento.
        return publish(RabbitTopology.FANOUT_EXCHANGE, "", request, Map.of());
    }

    public PublishResponse publishUnroutable(PublishRequest request) {
        log.info("Scrittura su unroutable exchange");
        // Fanout ignora la routing key; la stringa vuota rende esplicito il comportamento.
        return publish(RabbitTopology.UNROUTABLE_EXCHANGE, "", request, Map.of());
    }

    public PublishResponse publishTopic(PublishRequest request) {
        String key = requireRoutingKey(request.routingKey(), "ad esempio [order.created] oppure [order.cancelled]");
        log.info("Scrittura su topic exchange con routingKey={}", key);
        return publish(RabbitTopology.TOPIC_EXCHANGE, key, request, Map.of());
    }

    public PublishResponse publishHeaders(PublishRequest request) {
        Map<String, Object> headers = request.headers() == null ? Map.of() : request.headers();
        log.info("Scrittura su headers exchange con headers={}", headers);
        return publish(RabbitTopology.HEADERS_EXCHANGE, "", request, headers);
    }

    private PublishResponse publish(String exchange, String routingKey, PublishRequest request,
                                    Map<String, Object> headers) {
        MessagePostProcessor postProcessor = message -> {
            headers.forEach((name, value) -> message.getMessageProperties().setHeader(name, value));
            message.getMessageProperties().setContentType("text/plain");
            return message;
        };

        rabbitTemplate.convertAndSend(exchange, routingKey, request.message(), postProcessor);
        log.info("Scrittura su exchange={} routingKey={} headers={} message={}", exchange, routingKey, headers, request.message());
        return new PublishResponse(exchange, routingKey, request.message());
    }

    private String requireRoutingKey(String routingKey, String expected) {
        if (routingKey == null || routingKey.isBlank()) {
            throw new IllegalArgumentException("routingKey obbligatoria: " + expected);
        }
        return routingKey;
    }
}
