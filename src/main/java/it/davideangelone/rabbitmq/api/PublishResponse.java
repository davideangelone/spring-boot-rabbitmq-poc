package it.davideangelone.rabbitmq.api;

public record PublishResponse(String exchange, String routingKey, String message) {
}