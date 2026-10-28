package it.davideangelone.rabbitmq.consumer;

import java.nio.charset.StandardCharsets;

import it.davideangelone.rabbitmq.config.RabbitTopology;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MessageConsumers {

    @RabbitListener(queues = RabbitTopology.DIRECT_ORDERS_QUEUE)
    public void directOrders(Message message) {
        log.info("[consumer=direct-orders] {}", new String(message.getBody(), StandardCharsets.UTF_8));
    }

    @RabbitListener(queues = RabbitTopology.DIRECT_BILLING_QUEUE)
    public void directBilling(Message message) {
        log.info("[consumer=direct-billing] {}", new String(message.getBody(), StandardCharsets.UTF_8));
    }

    @RabbitListener(queues = RabbitTopology.FANOUT_EMAIL_QUEUE)
    public void fanoutEmail(Message message) {
        log.info("[consumer=fanout-email] {}", new String(message.getBody(), StandardCharsets.UTF_8));
    }

    @RabbitListener(queues = RabbitTopology.FANOUT_AUDIT_QUEUE)
    public void fanoutAudit(Message message) {
        log.info("[consumer=fanout-audit] {}", new String(message.getBody(), StandardCharsets.UTF_8));
    }

    @RabbitListener(queues = RabbitTopology.TOPIC_ORDER_CREATED_QUEUE)
    public void topicOrderCreated(Message message) {
        log.info("[consumer=topic-order-created] {}", new String(message.getBody(), StandardCharsets.UTF_8));
    }

    @RabbitListener(queues = RabbitTopology.TOPIC_ALL_ORDERS_QUEUE)
    public void topicAllOrders(Message message) {
        log.info("[consumer=topic-all-orders] {}", new String(message.getBody(), StandardCharsets.UTF_8));
    }

    @RabbitListener(queues = RabbitTopology.HEADERS_PDF_HIGH_QUEUE)
    public void headersPdfHigh(Message message) {
        log.info("[consumer=headers-pdf-high] {}", new String(message.getBody(), StandardCharsets.UTF_8));
    }

}
