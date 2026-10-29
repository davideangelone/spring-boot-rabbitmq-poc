# RabbitMQ Exchanges POC

POC con **Java 21**, **Spring Boot 4.1.1** e RabbitMQ. Implementa producer REST, consumer asincroni e i quattro tipi di exchange standard: `direct`, `fanout`, `topic` e `headers`. Include la gestione dei messaggi non instradabili (queue `queue.unroutable`) e una dead letter queue (`queue.dlq`).

## Componenti

- RabbitMQ `4.4.0` con Management UI, avviato tramite Docker Compose.
- Spring Boot applicativo eseguito localmente, con Swagger UI (springdoc-openapi) per provare le API.
- Topologia dichiarata da Spring all'avvio: exchange, queue e binding durevoli. **Ad ogni avvio l'applicazione cancella e ricrea** tutte le queue e gli exchange della topologia (vedi [Arresto e reset](#arresto-e-reset)).
- Consumer separati per rendere visibile il routing nei log e nella UI.

## Avvio

Prerequisiti: Docker Engine/Desktop con Compose v2, JDK 21 e Maven 3.6.3+.

1. Avvia RabbitMQ:

   ```bash
   docker compose up -d
   docker compose ps
   ```

2. Apri la UI di gestione: http://localhost:15672  
   Credenziali locali: `rabbit` / `rabbit`.

3. Avvia l'applicazione, dalla directory del progetto:

   ```bash
   mvn spring-boot:run
   ```

   L'applicazione ascolta su `http://localhost:8080`. In alternativa: `mvn clean package` e `java -jar target/rabbitmq-exchanges-poc-0.0.1-SNAPSHOT.jar`. Il comando `package` esegue anche i test, che richiedono RabbitMQ attivo (vedi [Test](#test)); aggiungi `-DskipTests` per saltarli.

4. Per provare le API da browser: Swagger UI su http://localhost:8080/swagger-ui.html (specifica OpenAPI su `/v3/api-docs`).

## Esempi producer

Gli endpoint rispondono `202 Accepted` con un JSON che riporta exchange, routing key e messaggio pubblicati, ad esempio:

```json
{"exchange":"exchange.direct","routingKey":"orders","message":"Ordine 123"}
```

La pubblicazione è asincrona: la risposta non dice se il messaggio sia stato effettivamente instradato verso una queue. I messaggi non instradabili si riconoscono dal log e dalla queue `queue.unroutable` (vedi [Messaggi non instradabili e DLQ](#messaggi-non-instradabili-e-dlq)).

### 1. Direct exchange

Il messaggio viene instradato alle code il cui binding key coincide esattamente con la routing key. La `routingKey` è obbligatoria.

```bash
curl -X POST http://localhost:8080/api/messages/direct ^
  -H "Content-Type: application/json" ^
  -d "{\"message\":\"Ordine 123\",\"routingKey\":\"orders\"}"
```

Valori ammessi dalla topologia di esempio: `orders` (queue `queue.direct.orders`) oppure `billing` (queue `queue.direct.billing`). Una routing key diversa non corrisponde a nessun binding: il messaggio non raggiunge le queue di questa topologia e finisce in `queue.unroutable`.

### 2. Fanout exchange

Il messaggio viene copiato verso tutte le queue associate all'exchange; la routing key viene ignorata.

```bash
curl -X POST http://localhost:8080/api/messages/fanout ^
  -H "Content-Type: application/json" ^
  -d "{\"message\":\"Evento broadcast\"}"
```

Il messaggio raggiunge sia `queue.fanout.email` sia `queue.fanout.audit`.

### 3. Topic exchange

La routing key (obbligatoria) viene confrontata con pattern di binding: `*` corrisponde a una parola, `#` a zero o più parole (separate da punti).

```bash
curl -X POST http://localhost:8080/api/messages/topic ^
  -H "Content-Type: application/json" ^
  -d "{\"message\":\"Ordine creato\",\"routingKey\":\"order.created\"}"
```

`order.created` raggiunge `queue.topic.order-created` e `queue.topic.all-orders`. Una routing key come `order.cancelled` raggiunge solo `queue.topic.all-orders`. Una routing key che non inizia con `order.` (ad esempio `invoice.created`) non corrisponde a nessun binding e finisce in `queue.unroutable`.

### 4. Headers exchange

Il routing dipende dagli header del messaggio, non dalla routing key. Il binding configurato richiede entrambi gli header `format=pdf` e `priority=high` (`x-match=all`).

```bash
curl -X POST http://localhost:8080/api/messages/headers ^
  -H "Content-Type: application/json" ^
  -d "{\"message\":\"Report PDF prioritario\",\"headers\":{\"format\":\"pdf\",\"priority\":\"high\"}}"
```

Se manca uno dei due header o il valore non coincide, il messaggio non raggiunge `queue.headers.pdf-high` e finisce in `queue.unroutable`.

> In PowerShell, `curl` può essere un alias di `Invoke-WebRequest`: se necessario, usa `curl.exe` oppure `Invoke-RestMethod`. Negli esempi sopra `^` è la continuazione riga di CMD; in bash/Linux/macOS sostituiscilo con `\`.

## Endpoint

| Metodo | Endpoint | Scopo |
|---|---|---|
| POST | `/api/messages/direct` | Pubblica sul direct exchange |
| POST | `/api/messages/fanout` | Pubblica sul fanout exchange |
| POST | `/api/messages/topic` | Pubblica sul topic exchange |
| POST | `/api/messages/headers` | Pubblica sul headers exchange |
| GET | `/swagger-ui.html` | Swagger UI |
| GET | `/v3/api-docs` | Specifica OpenAPI (JSON) |

Body JSON comune:

```json
{
  "message": "test",
  "routingKey": "order.created",
  "headers": {
    "format": "pdf",
    "priority": "high"
  }
}
```

Il campo `message` è sempre richiesto. I campi `routingKey` e `headers` sono utilizzati in funzione dell'exchange scelto: `routingKey` è obbligatoria per `direct` e `topic` (se assente o vuota la richiesta fallisce) e ignorata da `fanout` e `headers`; `headers` è usato solo dal `headers` exchange.

## Messaggi non instradabili e DLQ

**Messaggi non instradabili.** Il `RabbitTemplate` pubblica con il flag `mandatory`: se RabbitMQ non riesce a instradare un messaggio verso nessuna queue, lo restituisce all'applicazione, che lo ripubblica sul fanout exchange `exchange.unroutable` (collegato a `queue.unroutable`) e scrive un warning nel log con exchange e routing key originali. Prima della ripubblicazione aggiunge al messaggio gli header `x-original-exchange`, `x-original-routing-key` e `x-reply-text` (motivo restituito dal broker, ad esempio `NO_ROUTE`), visibili nella Management UI. Su `queue.unroutable` non c'è nessun consumer, quindi i messaggi restano consultabili.

E' stata usata una callback invece di un *alternate exchange* nativo di RabbitMQ in quanto l'alternate exchange è gestito interamente dal broker e l'applicazione non ne viene informata, quindi non potrebbe scrivere il log. Usando invece `mandatory` e la callback il broker restituisce il messaggio all'applicazione, che può loggarlo. 

I limiti di questa scelta sono:

- la ripubblicazione non è atomica: se l'applicazione o il broker si fermano tra la restituzione e il nuovo invio, il messaggio va perso (con un alternate exchange questo non accade);
- vengono intercettati solo i messaggi pubblicati da questo `RabbitTemplate`, non quelli di altri client sullo stesso broker.

**Dead letter queue.** Le sette queue applicative sono dichiarate con `x-dead-letter-exchange=exchange.dlx` e routing key `dead-letter`, che porta alla queue `queue.dlq`. Con `default-requeue-rejected: false`, un messaggio il cui listener solleva un'eccezione viene rifiutato senza rimessa in coda e quindi inoltrato alla DLQ. I consumer del POC si limitano a loggare il messaggio, perciò in uso normale `queue.dlq` resta vuota. Anche su `queue.dlq` non c'è nessun consumer.

## Osservare code e messaggi

Nella Management UI:

1. Apri **Exchanges** per vedere i sei exchange (i quattro di esempio più `exchange.unroutable` ed `exchange.dlx`) e i relativi binding.
2. Apri **Queues and Streams** per vedere le nove queue (le sette applicative più `queue.unroutable` e `queue.dlq`) e il numero di messaggi pronti/non confermati.
3. Per esaminare i messaggi, entra nella queue e usa **Get messages**. Su `queue.unroutable` e `queue.dlq` funziona direttamente, perché non hanno consumer. Sulle sette queue applicative i consumer sono attivi e prelevano i messaggi subito, quindi la queue risulta quasi sempre vuota: per vedere i messaggi accumulati bisogna disattivare temporaneamente i listener, ad esempio avviando l'applicazione con la property di Spring Boot `spring.rabbitmq.listener.simple.auto-startup=false`. Non fermare l'applicazione per accumulare messaggi: la pubblicazione passa dalle sue API REST e al riavvio le queue vengono cancellate e ricreate.
4. Controlla anche la console dell'applicazione: ogni listener identifica la queue/consumer che ha ricevuto il messaggio (`[consumer=direct-orders]`, `[consumer=fanout-email]`, ...) e i messaggi non instradabili generano un warning.

## Topologia dichiarata

| Exchange | Tipo | Queue | Binding |
|---|---|---|---|
| `exchange.direct` | direct | `queue.direct.orders` | `orders` |
| `exchange.direct` | direct | `queue.direct.billing` | `billing` |
| `exchange.fanout` | fanout | `queue.fanout.email` | tutte le routing key |
| `exchange.fanout` | fanout | `queue.fanout.audit` | tutte le routing key |
| `exchange.topic` | topic | `queue.topic.order-created` | `order.created` |
| `exchange.topic` | topic | `queue.topic.all-orders` | `order.#` |
| `exchange.headers` | headers | `queue.headers.pdf-high` | `format=pdf` AND `priority=high` |
| `exchange.unroutable` | fanout | `queue.unroutable` | tutte le routing key |
| `exchange.dlx` | direct | `queue.dlq` | `dead-letter` |

Le sette queue applicative (le prime sette righe) hanno `exchange.dlx` come dead letter exchange. Gli exchange e le queue sono durevoli.

## Test

```bash
mvn test
```

I test sono di integrazione (`@SpringBootTest`) e si collegano a un RabbitMQ reale su `localhost:5672` con le credenziali `rabbit` / `rabbit`: avvia prima `docker compose up -d`, altrimenti falliscono. Non usano un broker dedicato: lavorano sullo stesso RabbitMQ che usi per le prove manuali e, poiché l'avvio del contesto Spring cancella e ricrea la topologia, **cancellano i messaggi presenti nelle queue**.

## Arresto e reset

```bash
docker compose down
```

I dati del broker sono conservati nel volume Docker `rabbitmq_data`. Il contenuto delle queue della topologia, però, non sopravvive al riavvio dell'applicazione: all'avvio vengono cancellate e ricreate. Per eliminare anche i dati persistenti del broker e ripartire da zero:

```bash
docker compose down -v
```

**Nota sicurezza:** le credenziali sono volutamente semplici per un ambiente locale didattico; non riutilizzarle in ambienti condivisi o di produzione.
