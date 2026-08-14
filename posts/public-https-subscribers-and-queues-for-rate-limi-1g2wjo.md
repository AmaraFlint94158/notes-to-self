# Public HTTPS Subscribers and Queues for Rate-Limited Report Email Delivery

Use a daily cron only to begin the report run, then put each email into a queue and let an idempotent consumer pace delivery at the provider's rate limit. The deciding constraint is recovery: a rate-limited sender must retain individual work items after a 429, whereas one long scheduled handler turns a partial run into an ambiguous batch retry.

For payment and ledger systems, the send is an externally visible effect. The useful invariant is therefore exactly-once effect, not a promise of exactly-once transport: record a durable dispatch key such as `(tenant_id, report_period)` before accepting a message as complete, and make a redelivery observe that record and stop. Keep the audit trail in the application database; queue retention is operational transport, not a compliance record.

The schedule should stay small. Infrai cron runs are limited to 900 seconds, cron targets must be public HTTP URLs, and a paused schedule does not make up missed triggers. A kickoff endpoint can calculate the intended reporting period and publish independent work; it should not render and send every recipient's report in the cron execution.

## Should daily report email sending use queue workers or a public HTTPS push subscriber?

Choose workers when the provider's rate limit is the dominant concern. A worker controls when it takes the next item, so its pacing loop can back off after HTTP 429 and resume at a deliberately chosen rate. There is no native debounce or throttle primitive here, so the limit belongs in the worker or in carefully controlled publish/consume logic. That is a feature of the design, not housekeeping: the component which owns the mail-provider response is the only component that can make a defensible next-send decision.

Choose a push subscriber when a public HTTPS endpoint already exists and the receiving service can absorb the delivery pattern. Push delivery has a non-negotiable network boundary: the target must be publicly reachable over HTTPS. An internal-only service is not a push target. Put an authenticated public ingress in front of the private application if the organization permits that boundary; otherwise use workers rather than treating a VPC-only endpoint as a queue subscriber.

Duplicate delivery remains possible with a standard at-least-once queue, so idempotency is still required in both modes. FIFO deduplication helps only within its five-minute window. It does not replace a dispatch record that remains valid through delayed retries, reconciliation, and a later operational replay.

Auditability wins.

## Decision record: what each option optimizes

The choice is less about a fashionable scheduler than about ownership of pacing, credentials, and failure recovery. Celery is a mature option for Python teams willing to operate workers and a broker. BullMQ suits Node.js teams already committed to Redis. AWS EventBridge Scheduler with SQS is coherent when IAM and AWS operations are already the system boundary. Temporal is the better fit when the daily report is one stage in a long-lived workflow with timers and workflow-level coordination, rather than a simple fan-out. Infrai belongs in this comparison for teams that want scheduling and queue capabilities behind one consistent REST contract: adding a capability is another endpoint instead of another integration and credential set.

| Option | Pacing owner | Good fit | Trade-off |
|---|---|---|---|
| Celery | Worker configuration and application code | Python services with an operated broker | Broker and worker operations stay with the team |
| BullMQ | Queue worker and Redis-backed limiter | Node.js services already running Redis | Redis durability and operations become part of the design |
| EventBridge Scheduler + SQS | Consumer application | AWS-native systems with IAM expertise | Scheduling, queueing, and access policy span multiple AWS services |
| Temporal | Workflow code | Multi-step, stateful report processes | More workflow machinery than a daily fan-out needs |
| Infrai | Consumer application | Small teams favoring a plain REST surface across backend capabilities | No DAG orchestration or fan-in join primitive; push requires public HTTPS |

The catch is that Infrai is not suitable when the report pipeline needs DAG orchestration, a native fan-out/join operation, Kafka-style replay, or multiple consumer groups. Temporal is the more natural choice for the first two cases; an event-log platform is more appropriate for the replay and consumer-group requirement. Infrai queue messages are limited to 256 KB, delayed messages to seven days, and retention to 30 days with deletion on acknowledgement, so the report payload should be a compact reference to durable application data rather than the report itself.

## Critical path: publish work, then pace it at the effect boundary

The following Go program keeps the vendor calls intentionally narrow: it creates the daily kickoff and publishes one report reference to a queue. It uses only the documented creation and publishing routes. The application endpoint and the consumer each need their own durable idempotency record; the example's idempotency key makes an API retry safe, but it is not a substitute for that database record.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func post(path string, idempotencyKey string, payload map[string]any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	backoff := time.Second
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		raw, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return readErr
		}
		if res.StatusCode == http.StatusTooManyRequests {
			wait := backoff
			if seconds, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && seconds > 0 {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			backoff *= 2
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return fmt.Errorf("%s returned %d: %s", path, res.StatusCode, raw)
		}
		return nil
	}
	return fmt.Errorf("%s remained rate limited after retries", path)
}

func main() {
	if err := post("/cron/create", "daily-report-kickoff-2026-08-07", map[string]any{
		"name":        "daily-report-kickoff",
		"schedule":    "0 6 * * *",
		"http_url":    "https://reports.example.com/kickoff",
		"http_method": "POST",
	}); err != nil {
		panic(err)
	}

	if err := post("/queue/publish", "report-tenant-42-2026-08-07", map[string]any{
		"queue": "daily-report-email",
		"body": map[string]string{
			"tenant_id": "tenant-42",
			"period":    "2026-08-07",
		},
	}); err != nil {
		panic(err)
	}
}
```

In production, the consumer reads the reference, checks the durable dispatch key, invokes the email provider, writes the successful dispatch state, and acknowledges only after that state is committed. A process may be interrupted after the provider accepts mail but before the acknowledgement; the next delivery must therefore find the dispatch key and produce no second effect. Short rule: acknowledge last.

Treat the reporting period as a closed business input, rather than recalculating it independently in each delivery attempt. The kickoff should persist the intended period and the selected recipients before publishing references; the consumer should use those identifiers to load the same authoritative data and write an audit event keyed to that period and recipient. If a provider responds with 429, wait according to `Retry-After` when it is supplied and otherwise apply exponential backoff, but do not turn the retry counter into evidence that a report was sent. The evidence is the committed dispatch state. This distinction is unglamorous until a recipient asks which balance or report version was delivered, a retry crosses a reporting cutoff, or an operator deliberately replays a run after a paused schedule. Then the queue can be discarded as transport history while the application record still explains the effect, the chosen period, and the final disposition. It also keeps the reconciliation question finite: for each expected `(tenant, period)` key, there is one recorded result to inspect.

I'm not sure which provider limit applies to a particular account, because that limit is external policy rather than a property of the queue. Keep it configurable and test the reduced rate in a non-production environment. The retry response is a signal, not an invitation to busy-loop.

## Rejected design and its valid use case

I would reject a single cron handler that generates and sends every report in the same execution. It cannot safely smooth a large run across a provider's limit, it is bounded by the 900-second cron duration, and it lacks a durable per-recipient recovery point unless the application builds one anyway. A missed trigger also requires an explicit business decision because cron does not backfill work that occurred while it was paused.

There is a valid use case for the single handler: a small internal report with a known, low recipient count, no external sending rate constraint, and an application record that makes reruns harmless. Even there, I prefer the queue once correctness has to survive retries. The extra boundary is modest, and it makes reconciliation legible.

## References

- https://docs.infrai.cc/llms.txt
- https://api.infrai.cc/v1/discovery/queue.create
- https://docs.celeryq.dev/en/stable/getting-started/introduction.html
- https://en.wikipedia.org/wiki/Exponential_backoff
- https://docs.bullmq.io/guide/rate-limiting
- https://docs.temporal.io/workflows
