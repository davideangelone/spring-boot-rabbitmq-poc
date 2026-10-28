package it.davideangelone.rabbitmq;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import it.davideangelone.rabbitmq.api.PublishRequest;
import it.davideangelone.rabbitmq.api.PublishResponse;
import it.davideangelone.rabbitmq.config.RabbitTopology;
import it.davideangelone.rabbitmq.service.MessagePublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ExtendWith(SpringExtension.class)
class RabbitMqExchangesPocApplicationTests {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private MessagePublisher publisher;

    @Autowired
    private RabbitAdmin rabbitAdmin;

    @BeforeEach
    void purgeUnroutableQueue() {
        rabbitAdmin.purgeQueue(RabbitTopology.UNROUTABLE_QUEUE);
    }

    // Successful lifecycle tests
    @Test
    void directExchangeSuccessfulLifecycle() {
        String message = "test-direct-message";
        PublishRequest request = new PublishRequest(message, "orders", new HashMap<>());
        PublishResponse response = publisher.publishDirect(request);
        assertNotNull(response);
        assertEquals(RabbitTopology.DIRECT_EXCHANGE, response.exchange());
        assertEquals("orders", response.routingKey());
        assertEquals(message, response.message());
        // Message will be consumed by @RabbitListener, no need to manually consume
    }

    @Test
    void fanoutExchangeSuccessfulLifecycle() {
        String message = "test-fanout-message";
        PublishRequest request = new PublishRequest(message, "", new HashMap<>());
        PublishResponse response = publisher.publishFanout(request);
        assertNotNull(response);
        // Messages will be consumed by @RabbitListener, no need to manually consume
    }

    @Test
    void topicExchangeSuccessfulLifecycle() {
        String message = "test-topic-message";
        PublishRequest request = new PublishRequest(message, "order.created", new HashMap<>());
        PublishResponse response = publisher.publishTopic(request);
        assertNotNull(response);
        // Message will be consumed by @RabbitListener, no need to manually consume
    }

    @Test
    void headersExchangeSuccessfulLifecycle() {
        String message = "test-headers-message";
        Map<String, Object> headers = new HashMap<>();
        headers.put("format", "pdf");
        headers.put("priority", "high");
        PublishRequest request = new PublishRequest(message, "", headers);
        PublishResponse response = publisher.publishHeaders(request);
        assertNotNull(response);
        // Message will be consumed by @RabbitListener, no need to manually consume
    }

    // Sending failures tests
    @Test
    void directExchangeNullRoutingKeyThrowsException() {
        PublishRequest request = new PublishRequest("test", null, new HashMap<>());
        assertThrows(IllegalArgumentException.class,
                () -> publisher.publishDirect(request),
                "Dovrebbe lanciare eccezione per routing key null");
    }

    @Test
    void directExchangeEmptyRoutingKeyThrowsException() {
        PublishRequest request = new PublishRequest("test", "", new HashMap<>());
        assertThrows(IllegalArgumentException.class,
                () -> publisher.publishDirect(request),
                "Dovrebbe lanciare eccezione per routing key vuoto");
    }

    // DLQ tests
    @Test
    void directExchangeConsumerWrongKeyRoutesToUnroutable() throws Exception {
        String message = "test-unroutable-direct";
        rabbitTemplate.convertAndSend(RabbitTopology.DIRECT_EXCHANGE, "unknown-key", message);

        Thread.sleep(2000);

        boolean dlqHasMessage = waitForQueueMessage(RabbitTopology.UNROUTABLE_QUEUE, 3000);
        assertTrue(dlqHasMessage, "Il messaggio dovrebbe essere stato routato sulla unroutable");
    }

    @Test
    void topicExchangeConsumerAllOrdersTopic() {
        String message = "test-all-orders-topic-message";
        // Use a routing key that matches only topic-all-orders queue (order.#), not topic-order-created (order.created)
        PublishRequest request = new PublishRequest(message, "order.updated", new HashMap<>());
        PublishResponse response = publisher.publishTopic(request);
        assertNotNull(response);
        // Message will be consumed by @RabbitListener, no need to manually consume
    }

    // Unroutable messages tests
    @Test
    void unroutableMessageIsReturnedAndRepublishedToUnroutableQueue() {
        String body = "test-unroutable-" + UUID.randomUUID();

        // Nessun binding per "unknown-key": RabbitMQ restituisce il messaggio (mandatory)
        // e il return callback lo ripubblica su exchange.unroutable.
        publisher.publishDirect(new PublishRequest(body, "unknown-key", null));

        Message received = rabbitTemplate.receive(RabbitTopology.UNROUTABLE_QUEUE, 5000);

        assertNotNull(received, "Il messaggio non instradabile dovrebbe finire su queue.unroutable");
        assertEquals(body, new String(received.getBody(), StandardCharsets.UTF_8));
    }

    @Test
    void headersExchangeUnroutableMessage() throws Exception {
        String message = "test-unroutable-headers";
        Map<String, Object> headers = new HashMap<>();
        headers.put("format", "unknown");
        PublishRequest request = new PublishRequest(message, "", headers);
        PublishResponse response = publisher.publishHeaders(request);
        assertNotNull(response);

        Thread.sleep(3000);

        boolean received = waitForQueueMessage(RabbitTopology.UNROUTABLE_QUEUE, 5000);
        assertTrue(received, "Messaggio non routabile dovrebbe finire sulla queue unroutable");
    }

    // Helper methods
    private boolean waitForQueueMessage(String queueName, int timeoutMillis) throws Exception {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMillis) {
            Thread.sleep(100);
            Message message = rabbitTemplate.receive(queueName, 100);
            String received = message != null ? new String(message.getBody()) : null;
            if (received != null) {
                return true;
            }
        }
        return false;
    }
}