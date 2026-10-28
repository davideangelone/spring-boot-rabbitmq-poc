package it.davideangelone.rabbitmq;

import java.util.HashMap;
import java.util.Map;

import it.davideangelone.rabbitmq.api.PublishRequest;
import it.davideangelone.rabbitmq.api.PublishResponse;
import it.davideangelone.rabbitmq.config.RabbitTopology;
import it.davideangelone.rabbitmq.service.MessagePublisher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
@ActiveProfiles("test")
@ExtendWith(SpringExtension.class)
class RabbitMqExchangesPocApplicationTests {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private MessagePublisher publisher;

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

    // Consumer refusal tests
    @Test
    void consumerNackRejectDirect() throws Exception {
        String message = "test-nack-direct";
        rabbitTemplate.convertAndSend(RabbitTopology.DIRECT_EXCHANGE, "orders", message);

        Thread.sleep(1000);

        boolean queueHasMessage = waitForQueueMessage(RabbitTopology.DIRECT_ORDERS_QUEUE, 2000);
        assertFalse(queueHasMessage, "Il messaggio dovrebbe essere stato rifiutato e non reinserito");
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
    void unroutableExchangeWithMandatoryFlag() {
        String message = "test-unroutable";
        PublishRequest request = new PublishRequest(message, "", new HashMap<>());

        // The message is sent to an exchange with no bindings and mandatory=true
        // This test verifies the publisher can handle this scenario
        PublishResponse response = publisher.publishUnroutable(request);
        assertNotNull(response);
        assertEquals(RabbitTopology.UNROUTABLE_EXCHANGE, response.exchange());
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