package it.davideangelone.rabbitmq.config;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.amqp.autoconfigure.RabbitTemplateCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class RabbitConfiguration {

    // Header aggiunti ai messaggi ripubblicati su exchange.unroutable
    public static final String HEADER_ORIGINAL_EXCHANGE = "x-original-exchange";
    public static final String HEADER_ORIGINAL_ROUTING_KEY = "x-original-routing-key";
    public static final String HEADER_REPLY_TEXT = "x-reply-text";

    @Bean
    RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory, List<Queue> queues, List<Exchange> exchanges) {
        return new ResettingRabbitAdmin(connectionFactory, queues, exchanges);
    }

    private static class ResettingRabbitAdmin extends RabbitAdmin {

        private final List<Queue> queues;
        private final List<Exchange> exchanges;

        ResettingRabbitAdmin(ConnectionFactory connectionFactory, List<Queue> queues, List<Exchange> exchanges) {
            super(connectionFactory);
            this.queues = queues;
            this.exchanges = exchanges;
        }

        @Override
        public void afterPropertiesSet() {
            deleteQueues(queues);
            deleteExchanges(exchanges);

            // Le code, gli exchange e i binding verranno ricreati a partire dai rispettivi bean.
            super.afterPropertiesSet();
        }

        private void deleteQueues(List<Queue> queues) {
            queues.forEach(queue -> {
                log.info("Cancellazione coda: {}", queue.getName());
                deleteQueue(queue.getName());
            });
        }

        private void deleteExchanges(List<Exchange> exchanges) {
            // L'eventuale alternate exchange viene eliminato per ultimo.
            exchanges.forEach(exchange -> {
                log.info("Cancellazione exchange '{}'", exchange.getName());
                deleteExchange(exchange.getName());
            });
        }
    }

    @Bean
    public RabbitTemplateCustomizer rabbitTemplateCustomizer() {

        return template -> {
            template.setMandatory(true);

            template.setReturnsCallback(returned -> {
                Message message = returned.getMessage();
                String body = new String(message.getBody(), StandardCharsets.UTF_8);
                CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());

                log.warn("[{}] Inoltro su [{}] messaggio non instradabile: exchange=[{}], routingKey=[{}], message=[{}]",
                        correlationData.getId(),
                        RabbitTopology.UNROUTABLE_EXCHANGE,
                        returned.getExchange(),
                        returned.getRoutingKey(),
                        body
                );

                // Conserva il contesto originale: sulla queue.unroutable altrimenti resterebbero solo corpo e header.
                message.getMessageProperties().setHeader(HEADER_ORIGINAL_EXCHANGE, returned.getExchange());
                message.getMessageProperties().setHeader(HEADER_ORIGINAL_ROUTING_KEY, returned.getRoutingKey());
                message.getMessageProperties().setHeader(HEADER_REPLY_TEXT, returned.getReplyText());

                sendToUnroutableExchange(template, message, correlationData);
            });
        };
    }

    private void sendToUnroutableExchange(RabbitTemplate template, Message message, CorrelationData correlationData) {

        template.send(RabbitTopology.UNROUTABLE_EXCHANGE, "", message, correlationData);

        correlationData.getFuture().whenComplete((confirm, ex) -> {
            if (ex != null) {
                log.error("[{}] Errore durante la pubblicazione su queue.unroutable", correlationData.getId(), ex);
                return;
            }

            if (confirm.ack()) {
                if (correlationData.getReturned() == null) {
                    log.info("[{}] Messaggio confermato e instradato su queue.unroutable", correlationData.getId());
                } else {
                    log.error("[{}] Messaggio non instradato su queue.unroutable: replyText={}", correlationData.getId(), correlationData.getReturned().getReplyText());
                }
            } else {
                log.error("[{}] RabbitMQ ha rifiutato il messaggio: motivo={}", correlationData.getId(), confirm.reason());
            }
        });
    }

}
