package it.davideangelone.rabbitmq.config;

import java.util.Map;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitTopology {

    // DLQ per routing su DLQ
    private static final String DLQ_ROUTING_KEY = "dead-letter";

    // Exchanges
    public static final String DIRECT_EXCHANGE = "exchange.direct";
    public static final String FANOUT_EXCHANGE = "exchange.fanout";
    public static final String TOPIC_EXCHANGE = "exchange.topic";
    public static final String HEADERS_EXCHANGE = "exchange.headers";
    public static final String UNROUTABLE_EXCHANGE = "exchange.unroutable";
    private static final String DLX_EXCHANGE = "exchange.dlx";

    // Code
    public static final String DIRECT_ORDERS_QUEUE = "queue.direct.orders";
    public static final String DIRECT_BILLING_QUEUE = "queue.direct.billing";
    public static final String FANOUT_EMAIL_QUEUE = "queue.fanout.email";
    public static final String FANOUT_AUDIT_QUEUE = "queue.fanout.audit";
    public static final String TOPIC_ORDER_CREATED_QUEUE = "queue.topic.order-created";
    public static final String TOPIC_ALL_ORDERS_QUEUE = "queue.topic.all-orders";
    public static final String HEADERS_PDF_HIGH_QUEUE = "queue.headers.pdf-high";
    public static final String UNROUTABLE_QUEUE = "queue.unroutable";
    public static final String DLQ_QUEUE = "queue.dlq";

    /* Exchanges */
    @Bean
    DirectExchange directExchange() {
        return new DirectExchange(DIRECT_EXCHANGE, true, false);
    }

    @Bean
    FanoutExchange fanoutExchange() {
        return new FanoutExchange(FANOUT_EXCHANGE, true, false);
    }

    @Bean
    TopicExchange topicExchange() {
        return new TopicExchange(TOPIC_EXCHANGE, true, false);
    }

    @Bean
    HeadersExchange headersExchange() {
        return new HeadersExchange(HEADERS_EXCHANGE, true, false);
    }

    @Bean
    FanoutExchange unroutableExchange() {
        return new FanoutExchange(UNROUTABLE_EXCHANGE, true, false);
    }

    @Bean
    DirectExchange deadLetterExchange() {
        return new DirectExchange(DLX_EXCHANGE, true, false);
    }


    /* Code */
    // Code per direct exchange
    @Bean
    Queue directOrdersQueue() {
        return queueWithDlq(DIRECT_ORDERS_QUEUE);
    }

    @Bean
    Queue directBillingQueue() {
        return queueWithDlq(DIRECT_BILLING_QUEUE);
    }


    // Code per fanout exchange
    @Bean
    Queue fanoutEmailQueue() {
        return queueWithDlq(FANOUT_EMAIL_QUEUE);
    }

    @Bean
    Queue fanoutAuditQueue() {
        return queueWithDlq(FANOUT_AUDIT_QUEUE);
    }


    // Code per topic exchange
    @Bean
    Queue topicOrderCreatedQueue() {
        return queueWithDlq(TOPIC_ORDER_CREATED_QUEUE);
    }

    @Bean
    Queue topicAllOrdersQueue() {
        return queueWithDlq(TOPIC_ALL_ORDERS_QUEUE);
    }


    // Code per headers exchange
    @Bean
    Queue headersPdfHighQueue() {
        return queueWithDlq(HEADERS_PDF_HIGH_QUEUE);
    }

    // Coda per unroutable messages
    @Bean
    Queue unroutableQueue() {
        return new Queue(UNROUTABLE_QUEUE, true);
    }


    // Coda per DLQ
    @Bean
    Queue deadLetterQueue() {
        return new Queue(DLQ_QUEUE, true);
    }


    /* Bindings */
    // Bindings per direct exchange
    @Bean
    Binding directOrdersBinding(Queue directOrdersQueue, DirectExchange directExchange) {
        return BindingBuilder.bind(directOrdersQueue).to(directExchange).with("orders");
    }

    @Bean
    Binding directBillingBinding(Queue directBillingQueue, DirectExchange directExchange) {
        return BindingBuilder.bind(directBillingQueue).to(directExchange).with("billing");
    }


    // Bindings per fanout exchange
    @Bean
    Binding fanoutEmailBinding(Queue fanoutEmailQueue, FanoutExchange fanoutExchange) {
        return BindingBuilder.bind(fanoutEmailQueue).to(fanoutExchange);
    }

    @Bean
    Binding fanoutAuditBinding(Queue fanoutAuditQueue, FanoutExchange fanoutExchange) {
        return BindingBuilder.bind(fanoutAuditQueue).to(fanoutExchange);
    }


    // Bindings per topic exchange
    @Bean
    Binding topicOrderCreatedBinding(Queue topicOrderCreatedQueue, TopicExchange topicExchange) {
        return BindingBuilder.bind(topicOrderCreatedQueue).to(topicExchange).with("order.created");
    }

    @Bean
    Binding topicAllOrdersBinding(Queue topicAllOrdersQueue, TopicExchange topicExchange) {
        return BindingBuilder.bind(topicAllOrdersQueue).to(topicExchange).with("order.#");
    }


    // Bindings per headers exchange
    @Bean
    Binding headersPdfHighBinding(Queue headersPdfHighQueue, HeadersExchange headersExchange) {
        return BindingBuilder.bind(headersPdfHighQueue)
                .to(headersExchange)
                .whereAll(Map.of("format", "pdf", "priority", "high"))
                .match();
    }


    // Binding per unroutable queue
    @Bean
    public Binding unroutableBinding(Queue unroutableQueue, FanoutExchange unroutableExchange) {
        return BindingBuilder.bind(unroutableQueue).to(unroutableExchange);
    }


    // Binding per DLQ
    @Bean
    Binding deadLetterBinding(Queue deadLetterQueue, DirectExchange deadLetterExchange) {
        return BindingBuilder.bind(deadLetterQueue)
                .to(deadLetterExchange)
                .with(DLQ_ROUTING_KEY);
    }

    private Queue queueWithDlq(String queueName) {
        return QueueBuilder.durable(queueName)
                .deadLetterExchange(DLX_EXCHANGE)
                .deadLetterRoutingKey(DLQ_ROUTING_KEY)
                .build();
    }
}
