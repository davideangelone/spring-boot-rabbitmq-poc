package it.davideangelone.rabbitmq.api;

import java.util.Map;

public record PublishRequest(String message, String routingKey, Map<String, Object> headers) {
}