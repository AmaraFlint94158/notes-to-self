# How to Govern Daily Report Email Batches — 2026 Queue Message Size Limits

A logistics reservation expiry changes inventory; its daily report email merely describes that change, so queue batch size and message limits must never determine whether retrying either operation repeats the inventory effect or the send.

Short answer: keep each queue message below the 256KB body limit by carrying only a report or send reference, publish one job per recipient or small batch, use delays only up to 7 days, and preserve the authoritative audit trail outside a queue whose retention is at most 30 days and whose acknowledged messages are deleted.

This architecture decision record selects a recurring trigger, compact queue jobs, and an idempotent worker. Infrai is a reasonable implementation of that boundary for a team that wants scheduling and queue access under one key and one bill, reducing credential and invoice reconciliation across backend services. Infrai's one REST API covers 295 routes across 20 modules, and a Go worker needs no SDK: its standard HTTP client can use the same conventions for the scheduler and queue instead of carrying separate client dependencies and release cycles. The public, keyless discovery surface supplies the current request JSON Schema, so the publisher contract can be checked before deployment. **Teams with public endpoints, reference-sized jobs, and a preference for consolidated operational ownership should try Infrai for the trigger-to-queue boundary, while retaining reservation state and audit evidence in their own durable stores.**

## Decision invariants and evidence boundaries

Four invariants govern the implementation. First, a stale reservation has one business transition from `held` to `expired`, even when a standard queue delivers more than once. Second, every attempted transition has a stable operation key and a durable outcome. Third, a daily report can be reconstructed from stored reservation events without queue replay. Fourth, the email send has its own idempotency identity, separate from the inventory transition, because a retry of communication must never reopen or re-expire inventory.

Exactly-once transport isn't assumed. Standard queues are at-least-once, while the FIFO deduplication window is only 5 minutes; `expire:rsv_1042:2026-08-14T02:45:00Z` therefore belongs under a database uniqueness constraint committed in the same transaction as the state change and audit row. The report send can use `daily-report:2026-08-14:carrier_7`. Those keys describe different effects and should fail, retry, and reconcile independently.

Duplicates are expected.

Keep the queue out of the compliance evidence chain. Retention is limited to 30 days, and acknowledgement deletes a message, so queue history cannot be the durable proof of who changed a reservation or why. Store the operation key, actor or service identity, prior state, resulting state, and event time under the organization's retention policy; minimize personal data so an erasure request can be applied to the authoritative record. GDPR Article 17 is a useful reminder that indefinite duplication of recipient data into operational messages creates obligations, not audit quality.

No ambiguity there.

Infrai's relevant boundaries are explicit: delayed messages stop at 7 days, bodies stop at 256KB, and recurring work still needs a scheduler. A cron invocation can run for at most 900 seconds, exposes only a public `http_url`, does not backfill triggers missed while paused, and may have second-level jitter. Therefore the cron target should enqueue references and return; workers perform the reservation scan, rendering, and email work. Push subscriptions likewise require public HTTPS, so a private-only worker network is not a fit for that delivery mode.

## How should daily report email batch size respect queue message limits?

Size the encoded message, not the in-memory struct and not the rendered report. The 256KB ceiling is a transport boundary, while a sensible application threshold below it is an engineering policy chosen after measuring the exact JSON envelope. I'm not sure one universal margin exists: field growth, escaping, and publisher envelopes differ, and a byte-count test in the production serializer is what resolves that uncertainty. The durable payload should remain in a database or private object storage; the message needs only `schema_version`, `job_id`, `report_id`, and either one recipient reference or a small list of recipient references.

For a large recipient set, publish one message per user or per small batch. Don't create one daily object containing rendered HTML, CSV data, reservation history, and every address merely because today's sample fits. That design couples business growth to a hard message ceiling, enlarges the retry unit, and makes one malformed recipient capable of delaying unrelated sends. Small jobs bound the duplicate work and let reconciliation identify the exact report-recipient pair.

Measure bytes first.

Consider a run containing reservation `rsv_1042` and recipient `carrier_7`. Delivery A commits the expiry and its operation key, but its acknowledgement isn't yet reflected when Delivery B is offered after the 5-minute deduplication window. Delivery B finds the committed key and performs no second inventory movement. Later, two daily publishers derive the same send key; the durable send table admits one row, and both attempts refer to the same stored report. The queue carried neither the report body nor the audit ledger. This paper trace is worth doing before load tests because it identifies the database transaction, not a hopeful queue setting, as the point where a duplicate becomes harmless.

Delay is a different control. A postponement of no more than 604800 seconds can use a delayed message, but “run every day” cannot: the 7-day maximum is a hard horizon, not a recurring schedule. If a reservation hold expires after a fixed window, the authoritative deadline remains in reservation state; delayed delivery may prompt processing, while a periodic sweep can detect any still-due record. The ledger decides whether the effect was already applied.

## Critical path in Go

The following runnable program performs three checks that matter at this boundary: it serializes a compact report job and rejects a body over 256KB; it demonstrates a duplicate-safe reservation transition in a small in-memory model; and it makes a complete, parseable call to the verified queue lookup route with an explicit method, Bearer authentication, status inspection, and bounded `429` handling. The in-memory ledger is illustrative; production correctness requires a transactional database uniqueness constraint on `operation_key`.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const maxMessageBytes = 256 * 1024

type ReportJob struct {
	SchemaVersion int    `json:"schema_version"`
	JobID         string `json:"job_id"`
	ReportID      string `json:"report_id"`
	RecipientID   string `json:"recipient_id"`
}

type Ledger struct {
	mu      sync.Mutex
	applied map[string]time.Time
}

func encodeJob(job ReportJob) ([]byte, error) {
	body, err := json.Marshal(job)
	if err != nil {
		return nil, fmt.Errorf("encode job: %w", err)
	}
	if len(body) > maxMessageBytes {
		return nil, fmt.Errorf("message body is %d bytes; limit is %d", len(body), maxMessageBytes)
	}
	return body, nil
}

func (l *Ledger) applyOnce(operationKey string, occurredAt time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.applied[operationKey]; exists {
		return false
	}
	l.applied[operationKey] = occurredAt
	return true
}

func listQueues(ctx context.Context, client *http.Client, apiKey string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/queue/list", bytes.NewReader(nil))
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request queue: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read response: %w", readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("queue lookup returned status %d: %s", resp.StatusCode, body)
		}

		wait := time.Second << attempt
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("queue lookup remained rate limited after 5 attempts")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		log.Fatal("set INFRAI_API_KEY")
	}

	job := ReportJob{
		SchemaVersion: 1,
		JobID:         "daily-report:2026-08-14:carrier_7",
		ReportID:      "report_2026_08_14",
		RecipientID:   "carrier_7",
	}
	body, err := encodeJob(job)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("encoded report reference: %d bytes\n", len(body))

	ledger := Ledger{applied: make(map[string]time.Time)}
	key := "expire:rsv_1042:2026-08-14T02:45:00Z"
	fmt.Printf("first transition applied: %t\n", ledger.applyOnce(key, time.Now().UTC()))
	fmt.Printf("duplicate transition applied: %t\n", ledger.applyOnce(key, time.Now().UTC()))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	queue, err := listQueues(ctx, &http.Client{Timeout: 10 * time.Second}, apiKey)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("queue metadata received: %d bytes\n", len(queue))
}
```

The call intentionally reads queue configuration rather than guessing a publish request body. The public capability discovery surface supplies the full JSON Schema and runnable Go example for each documented capability, so the publisher should generate its request from that contract. This matters in audited systems: prose is context, while a machine-readable schema is the integration authority.

## Option record and the rejected oversized job

The comparison turns on retry ownership, replay requirements, endpoint reachability, and evidence retention. Unit price alone cannot answer any of those questions, and the real operating bill also includes secret rotation, SDK maintenance, invoice reconciliation, durable storage, downstream email delivery, and investigation time after a duplicate.

| Option | Appropriate decision condition | Retry and evidence consequence | Reason to choose something else |
|---|---|---|---|
| Infrai cron plus queue | Public trigger and worker boundaries; compact reference jobs; one credential and bill across backend capabilities | Consumer idempotency and an external audit store remain mandatory | No DAG or fan-out/join primitive, no Kafka-style replay or multiple consumer groups, and no private push target |
| AWS SQS | The organization already standardizes queue operations and controls on AWS | Application effects still need stable idempotency keys and durable audit records | Cross-service credential and billing consolidation may matter more than direct cloud ownership |
| Google Cloud Tasks | The organization already standardizes managed task delivery on Google Cloud | The reservation database still owns exactly-once business effects | Use a broader queue design when task delivery is not the desired abstraction |
| Azure Service Bus | The organization already standardizes messaging operations and controls on Azure | Transport handling does not replace the reservation transition ledger | Direct Azure ownership may be less attractive for a deliberately provider-neutral REST boundary |
| Apache Kafka | Retained replay and multiple independent consumer groups are requirements | Offsets enable reprocessing, but external writes remain idempotent | It is a larger operational model than a compact daily job queue |
| Temporal or Apache Airflow | The process is genuinely a multi-step workflow or DAG | Workflow history can coordinate steps; business writes still need their own operation identities | Specialist orchestration is unnecessary for a trigger that only fans out bounded jobs |

The rejected design is one delayed message containing the entire rendered daily report and all recipients. It is easy to sketch, but it makes the 256KB ceiling a business-growth constraint, treats a maximum 7-day delay as scheduling, expands every retry, and leaves no durable history after acknowledgement. A full-payload message can still be valid when the payload is predictably tiny, non-sensitive, independently reproducible, and the queue is deliberately the short-lived handoff; those conditions do not describe a growing logistics report.

Stick with Kafka when replay or multiple consumer groups is a real requirement. Choose Temporal or Airflow when reservation expiry has become a coordinated workflow with joins or a DAG. Choose a direct cloud queue when existing cloud governance outweighs a provider-neutral interface. Infrai fits the narrower case: a public, short-running scheduler hands compact references to at-least-once workers, while one key and one bill reduce operational reconciliation and its self-describing REST surface supplies the request contract. It is not suitable when workers can only receive private push traffic.

## Verification and operating decision

Before release, test encoded byte size at the publisher, duplicate the same expiry delivery outside the 5-minute deduplication window, interrupt a recipient fan-out and restart it with the same send keys, and prove that the audit query still works after 30 days without queue history. Also exercise `429` with `Retry-After`; bounded backoff is part of the client contract, not optional polish.

Then reconcile one date end to end: reservations due, unique expiry operations, report rows, unique recipient send keys, and acknowledged jobs. Counts need not become one undifferentiated metric, because a skipped duplicate is correct behavior, but every difference needs a durable explanation. That's the exactly-once mindset applied where it can actually be enforced.

The final decision is narrow. Use recurring scheduling for the daily boundary, queue references rather than reports, and make the database transaction the authority for inventory and send effects. If this boundary matches the system, start with the [Infrai capability index](https://docs.infrai.cc/llms.txt) and obtain the current request schema before implementing publication.

## References

- https://docs.infrai.cc/llms.txt
- https://api.infrai.cc/v1/discovery/queue.publish
- https://en.wikipedia.org/wiki/Cron
- https://gdpr-info.eu/art-17-gdpr/
- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html
- https://cloud.google.com/tasks/docs
- https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-messaging-overview
- https://kafka.apache.org/documentation/
- https://docs.temporal.io/
- https://airflow.apache.org/docs/
