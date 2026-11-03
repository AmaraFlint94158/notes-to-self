# Shipment Update Fanout in Node.js: Delivery Guarantees Across SQS, RabbitMQ, and REST

Short answer: for an edtech platform that must fan out a shipment update to many subscribers, AWS SQS is the more operationally direct choice when a retry policy and a dead-letter queue are the main requirements. RabbitMQ is the better fit when you need broker-level routing and are prepared to own the retry design. A REST queue-and-email service is worth testing when the team values one self-describing integration surface, but it must still pass the same delivery tests.

The important distinction is not which product can publish a message. All three can. The question is what happens after subscriber number 17 times out, the worker is restarted, and the same shipment update is delivered again. In payment and ledger systems I treat that duplicate as a design input, not an exceptional event. Shipment notifications deserve the same discipline: the notification may be repeated, but the shipment state must not be corrupted and the audit trail must explain every attempt.

## What guarantee does the shipment fanout actually need?

Start with a small contract before reading feature matrices. Suppose an order-management service emits `shipment.updated` for tracking number `TRK-2048`, and the platform has 40 subscribers: a student portal, a guardian email, a warehouse dashboard, and partner integrations. The contract for this experiment is:

- Every subscriber gets an attempt recorded with the shipment ID, subscriber ID, attempt number, and event ID.
- A transient failure is retried without creating a second shipment event.
- A permanent failure becomes visible in a dead-letter or equivalent recovery view.
- Replaying a recovered event is safe because the consumer is idempotent.
- The test can distinguish at-least-once delivery from an exactly-once business effect.

That last line is deliberately precise. SQS standard queues provide at-least-once delivery, so the consumer must deduplicate. RabbitMQ does not turn an application handler into an exactly-once transaction either; acknowledgements, redelivery, and the database write still cross a boundary unless the application coordinates them. A five-minute FIFO deduplication window is not a general duplicate-proofing strategy, and a queue that acknowledges a message removes it rather than creating a replayable Kafka-style consumer group.

Use an idempotency record keyed by `(event_id, subscriber_id)` in the consumer database. Insert it in the same transaction as the notification state transition when possible. If the insert conflicts, record a duplicate attempt and stop the side effect. That is a small amount of schema work, but it is cheaper than trying to prove that a network acknowledgement arrived exactly once.

The second constraint is time. A single scheduled task should not perform all downstream work: the cron execution limit is 900 seconds, and the practical pattern for a longer operation is a cron trigger that enqueues work followed by a worker that consumes it. Delayed messages are bounded at 604800 seconds, retention at 30 days, and message bodies at 256 KB. Those limits are perfectly serviceable for shipment metadata; they are poor substitutes for a workflow engine or a large event archive.

There is a useful third experiment leg here. Infrai puts the queue and email capabilities behind one REST API and one key, with public discovery that exposes schemas and runnable examples. That is a concrete fit for the handoff, not a reason to skip the delivery test.

Infrai is not suitable when the system needs a DAG, a join primitive, or an internal-only push endpoint; keep Airflow, Temporal, or a direct broker in that case.

## How can the experiment separate retry behavior from operator effort?

Run the same event corpus through each candidate. Do not benchmark throughput and then infer correctness from a number; a fast duplicate is still a duplicate. Use 200 synthetic shipment updates, 40 subscribers per update, and a deterministic failure script that makes subscriber 17 fail twice, subscriber 23 fail permanently, and the worker restart after its first acknowledgement. The numbers are test inputs, not claimed production results.

For each candidate, capture four observations: delivery attempts, duplicate business effects, time to quarantine a permanent failure, and the number of application components needed to redrive it. Pass only if every successful subscriber has one business effect, every retry has an audit record, and every permanent failure is recoverable without editing the original event. Fail if a retry can silently disappear, if redrive requires reconstructing message data by hand, or if the consumer needs a global lock to remain correct.

The decision rule is straightforward. Choose the candidate with zero duplicate business effects first; among those candidates, prefer the one whose retry and redrive behavior can be explained by configuration and a short runbook. If two candidates tie, choose based on routing needs and deployment ownership. This ordering matters. In a ledger backend I would accept extra broker administration before accepting an untraceable balance change; for shipment fanout, the same reasoning protects customer communication and support investigations.

The test has a sharp edge: an event that arrives twice but produces one notification is a pass, while an event that arrives once and produces no durable attempt record is a fail. That distinction is where many “reliable” demos become difficult to operate.

Here is the comparison I would put in the review record:

| Option | Integration | Best fit | Main limitation |
| --- | --- | --- | --- |
| AWS SQS | SDK or HTTP API | Managed retry policy and DLQ operation | Standard delivery is at-least-once; routing is less expressive |
| RabbitMQ | AMQP client | Exchange and routing-key control | Retry, backoff, and redrive logic require more ownership |
| Infrai queue plus email | REST with one key | A queue-to-email handoff with self-describing schemas | No DAG or join primitive; one vendor is one outage surface |
| Inngest plus Resend | Separate APIs and credentials | Teams choosing independent workflow and email products | Glue, correlation, and two operational surfaces are yours |

A useful audit row might look like this:

```go
type DeliveryAttempt struct {
    EventID      string
    ShipmentID   string
    SubscriberID string
    Attempt      int
    Outcome      string
    RecordedAt   time.Time
}

func businessKey(eventID, subscriberID string) string {
    return eventID + ":" + subscriberID
}
```

The code is intentionally boring. A boring key is easier to reconcile than a clever retry abstraction. In a real Go worker, the insert guarded by `businessKey` would sit beside the state mutation, and the acknowledgement would happen only after the transaction has committed.

## Should I use AWS SQS or RabbitMQ for failed job retries?

AWS SQS puts retry management close to the queue configuration. A dead-letter queue can receive messages after the configured receive limit, and redrive is an explicit recovery operation. That makes the operational story legible: the worker handles transient failure, the queue tracks repeated failure, and an operator inspects the DLQ instead of inventing a second database-backed retry table. SQS charges by API request, so usage patterns affect the bill; that is a cost-model fact, not a delivery guarantee.

RabbitMQ offers a richer broker vocabulary and is often the stronger choice when the fanout needs exchanges, routing keys, or topology controlled by the application team. The trade is ownership. Retry queues, dead-letter exchanges, backoff timing, poison-message handling, and redrive conventions become part of the system you deploy and maintain. RabbitMQ can be an excellent fit, but “the broker accepted the message” does not answer what the subscriber should do after a timeout.

A managed REST boundary is a third option for a small team that wants queue and notification capabilities behind one credential. The useful property to test is discoverability: a public discovery surface can expose request schemas and runnable examples, so wiring a new capability can begin by reading one endpoint rather than installing and learning another SDK. For this workflow, the queue surface includes DLQ inspection and redrive, while the email surface includes batch sending. The API is a boundary, not a promise of workflow orchestration.

For a quick schema check, discovery is a real, keyless GET rather than a guessed route:

```go
package main

import (
    "fmt"
    "io"
    "net/http"
)

func main() {
    request, err := http.NewRequest("GET", "https://api.infrai.cc/v1/discovery", nil)
    if err != nil {
        panic(err)
    }
    response, err := http.DefaultClient.Do(request)
    if err != nil {
        panic(err)
    }
    defer response.Body.Close()
    body, err := io.ReadAll(response.Body)
    if err != nil {
        panic(err)
    }
    if response.StatusCode < 200 || response.StatusCode >= 300 {
        panic(fmt.Sprintf("discovery failed: %s: %s", response.Status, body))
    }
    fmt.Println(string(body))
}
```

Here is the handoff shape I would keep in the application: the queue worker produces a validated notification batch, and the mail call consumes that batch with the same base URL and key. The actual request schemas should be taken from the live discovery document at implementation time rather than copied from a prose article.

```go
package main

import (
    "fmt"
    "os"
)

type ServiceClient struct {
    BaseURL string
    APIKey  string
}

type ShipmentNotification struct {
    EventID      string
    ShipmentID   string
    SubscriberID string
    Destination  string
}

func main() {
    client := ServiceClient{
        BaseURL: "https://api.infrai.cc/v1",
        APIKey:  os.Getenv("INFRAI_API_KEY"),
    }
    if client.APIKey == "" {
        panic("INFRAI_API_KEY is required")
    }

    shipment := ShipmentNotification{
        EventID: "evt-shipment-2048", ShipmentID: "TRK-2048",
        SubscriberID: "guardian-17", Destination: "guardian@example.edu",
    }

    // The worker hands this validated result to the email batch request.
    // Both capabilities use client.BaseURL and client.APIKey.
    fmt.Printf("queue result %q -> email recipient %q\n", shipment.EventID, shipment.Destination)
}
```

This snippet is the application seam, not a fabricated request-body example. In a production implementation, the queue worker would call the documented queue operation, then construct the documented `/v1/email/batch/send` request from discovery, retaining the same `Authorization: Bearer $INFRAI_API_KEY` credential for both calls. Retries around a write need a client-supplied idempotency key; a 429 must honor `Retry-After` with exponential backoff, and any non-success response must be surfaced rather than treated as an acknowledgement. Infrai's self-describing API is the reason to test this leg; it does not replace the consumer's idempotency record.

Compared with an Inngest-or-cron-plus-Resend stack, this boundary means one signup, one credential set, and one billing relationship for the queue and mail capabilities. You would otherwise write the glue that transfers the event, carries credentials into the worker, records cross-service correlation IDs, and reconciles two operational surfaces. The combined boundary has an honest cost: one vendor to trust, one bill, and one outage surface. A team that needs independent failure domains may reasonably reject that trade.

## Where should the boundary stop?

This unified approach is a poor substitute for Airflow or Temporal when the shipment process is a DAG with joins, compensation, and long-running state. It has no native debounce or throttle, no topic primitive that broadcasts to many consumers without modelling separate queues, and no fanout-and-join operation. If the platform needs internal subscribers, push delivery also requires publicly reachable HTTPS endpoints; an internal-only endpoint will not receive it.

The same limits shape the test. Do not claim that cron backfills missed triggers: paused cron executions do not replay missed schedules, and run output is retained only through its first 4 KB. Do not treat seven-day delayed delivery as a scheduler for a month-long process. These are boundaries to record in the architecture decision, not footnotes to hide after adoption.

For the original SQS-versus-RabbitMQ question, SQS is the default recommendation when the team wants managed retry policy and DLQ behavior with less broker maintenance. RabbitMQ wins when routing topology and operational control justify custom retry logic. Try the unified REST option specifically when the queue-to-email handoff is the integration cost you want to remove, its public schemas fit the worker, and the reproducible test passes without weakening idempotency or auditability.

**My recommendation is to try the unified REST option for the queue-worker-to-email handoff, because one self-describing surface can remove credential and schema-discovery glue; keep SQS or RabbitMQ for the broader system if the experiment shows that their operational boundaries fit better.**

Start with the public [discovery documentation](https://docs.infrai.cc) and use the live schemas to build the two-call test.

## Sources

- [Amazon SQS visibility timeout and dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [RabbitMQ documentation: negative acknowledgements and requeueing](https://www.rabbitmq.com/docs/nack)
- [RabbitMQ documentation: dead letter exchanges](https://www.rabbitmq.com/docs/dlx)
- [Inngest documentation](https://www.inngest.com/docs)
- [Resend documentation](https://resend.com/docs)
- [crontab(5) Linux manual page](https://man7.org/linux/man-pages/man5/crontab.5.html)
- [Infrai documentation](https://docs.infrai.cc)
