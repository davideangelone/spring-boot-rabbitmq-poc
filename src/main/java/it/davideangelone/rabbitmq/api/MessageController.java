package it.davideangelone.rabbitmq.api;

import it.davideangelone.rabbitmq.service.MessagePublisher;
import jakarta.validation.Valid;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/messages")
@Tag(name = "RabbitMQ Producer", description = "Pubblica messaggi sui quattro tipi di exchange RabbitMQ")
public class MessageController {

    private final MessagePublisher publisher;

    public MessageController(MessagePublisher publisher) {
        this.publisher = publisher;
    }

    @Operation(summary = "Pubblica su direct exchange", description = "La routing key deve coincidere con il binding key: orders oppure billing.")
    @PostMapping("/direct")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PublishResponse direct(@Valid @RequestBody PublishRequest request) {
        return publisher.publishDirect(request);
    }

    @Operation(summary = "Pubblica su fanout exchange", description = "Distribuisce il messaggio a tutte le queue associate; la routing key viene ignorata.")
    @PostMapping("/fanout")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PublishResponse fanout(@Valid @RequestBody PublishRequest request) {
        return publisher.publishFanout(request);
    }

    @Operation(summary = "Pubblica su topic exchange", description = "Il routing dipende dalla routing key e dai pattern dei binding, ad esempio order.created.")
    @PostMapping("/topic")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PublishResponse topic(@Valid @RequestBody PublishRequest request) {
        return publisher.publishTopic(request);
    }

    @Operation(summary = "Pubblica su headers exchange", description = "Il binding di esempio richiede gli header format=pdf e priority=high.")
    @PostMapping("/headers")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PublishResponse headers(@Valid @RequestBody PublishRequest request) {
        return publisher.publishHeaders(request);
    }
}
