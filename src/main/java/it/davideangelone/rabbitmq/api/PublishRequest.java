package it.davideangelone.rabbitmq.api;

import java.util.Map;

import jakarta.validation.constraints.NotBlank;

public record PublishRequest(@NotBlank String message, String routingKey, Map<String, Object> headers) {
}