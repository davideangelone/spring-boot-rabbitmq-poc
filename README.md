# RabbitMQ Exchanges POC

POC con **Java 21**, **Spring Boot 4.1.1** e RabbitMQ. Implementa producer REST, consumer asincroni e i quattro tipi di exchange standard: `direct`, `fanout`, `topic` e `headers`.

## Componenti

- RabbitMQ `4.4.0` con Management UI, avviato tramite Docker Compose.
- Spring Boot applicativo eseguito localmente.
- Topologia dichiarata da Spring all'avvio: exchange, queue e binding durevoli.
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

   L'applicazione ascolta su `http://localhost:8080`. In alternativa: `mvn clean package` e `java -jar target/rabbitmq-exchanges-poc-0.0.1-SNAPSHOT.jar`.

## Esempi producer

### 1. Direct exchange

Il messaggio viene instradato alle code il cui binding key coincide esattamente con la routing key.

```bash
curl -X POST http://localhost:8080/api/messages/direct ^
  -H "Content-Type: application/json" ^
  -d "{\"message\":\"Ordine 123\",\"routingKey\":\"orders\"}"
```

Valori ammessi dalla topologia di esempio: `orders` (queue `q.direct.orders`) oppure `billing` (queue `q.direct.billing`). Una routing key diversa non corrisponde a nessun binding e il messaggio non raggiunge le queue di questa topologia.

### 2. Fanout exchange

Il messaggio viene copiato verso tutte le queue associate all'exchange; la routing key viene ignorata.

```bash
curl -X POST http://localhost:8080/api/messages/fanout ^
  -H "Content-Type: application/json" ^
  -d "{\"message\":\"Evento broadcast\"}"
```

Il messaggio raggiunge sia `q.fanout.email` sia `q.fanout.audit`.

### 3. Topic exchange

La routing key viene confrontata con pattern di binding: `*` corrisponde a una parola, `#` a zero o più parole (separate da punti).

```bash
curl -X POST http://localhost:8080/api/messages/topic ^
  -H "Content-Type: application/json" ^
  -d "{\"message\":\"Ordine creato\",\"routingKey\":\"order.created\"}"
```

`order.created` raggiunge `q.topic.order-created` e `q.topic.all-orders`. Una routing key come `order.cancelled` raggiunge solo `q.topic.all-orders`.

### 4. Headers exchange

Il routing dipende dagli header del messaggio, non dalla routing key. Il binding configurato richiede entrambi gli header `format=pdf` e `priority=high` (`x-match=all`).

```bash
curl -X POST http://localhost:8080/api/messages/headers ^
  -H "Content-Type: application/json" ^
  -d "{\"message\":\"Report PDF prioritario\",\"headers\":{\"format\":\"pdf\",\"priority\":\"high\"}}"
```

Se manca uno dei due header o il valore non coincide, il messaggio non raggiunge `q.headers.pdf-high`.

> In PowerShell, `curl` può essere un alias di `Invoke-WebRequest`: se necessario, usa `curl.exe` oppure `Invoke-RestMethod`. Negli esempi sopra `^` è la continuazione riga di CMD; in bash/Linux/macOS sostituiscilo con `\`.

## Endpoint

| Metodo | Endpoint | Scopo |
|---|---|---|
| POST | `/api/messages/direct` | Pubblica sul direct exchange |
| POST | `/api/messages/fanout` | Pubblica sul fanout exchange |
| POST | `/api/messages/topic` | Pubblica sul topic exchange |
| POST | `/api/messages/headers` | Pubblica sul headers exchange |

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

I campi `routingKey` e `headers` sono utilizzati in funzione dell'exchange scelto.

## Osservare code e messaggi

Nella Management UI:

1. Apri **Exchanges** per vedere i quattro exchange e i relativi binding.
2. Apri **Queues and Streams** per vedere le sette queue e il numero di messaggi pronti/non confermati.
3. Per esaminare i messaggi, entra nella queue e usa **Get messages**. I consumer dell'applicazione sono attivi: normalmente prelevano i messaggi rapidamente, quindi per osservarli nella coda puoi fermare temporaneamente l'app (`Ctrl+C`), pubblicare i messaggi e poi riavviarla.
4. Controlla anche la console dell'applicazione: ogni listener identifica la queue/consumer che ha ricevuto il messaggio.

## Topologia dichiarata

| Exchange | Queue | Binding |
|---|---|---|
| `poc.direct` | `q.direct.orders` | `orders` |
| `poc.direct` | `q.direct.billing` | `billing` |
| `poc.fanout` | `q.fanout.email` | tutte le routing key |
| `poc.fanout` | `q.fanout.audit` | tutte le routing key |
| `poc.topic` | `q.topic.order-created` | `order.created` |
| `poc.topic` | `q.topic.all-orders` | `order.#` |
| `poc.headers` | `q.headers.pdf-high` | `format=pdf` AND `priority=high` |

## Arresto e reset

```bash
docker compose down
```

I dati sono conservati nel volume Docker `rabbitmq_data`. Per eliminare anche dati persistenti e ripartire da zero:

```bash
docker compose down -v
```

**Nota sicurezza:** le credenziali sono volutamente semplici per un ambiente locale didattico; non riutilizzarle in ambienti condivisi o di produzione.
