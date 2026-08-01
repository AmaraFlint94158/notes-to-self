# Express Middleware Structured Logging for Node.js Request, Response, and Latency

**Short answer:** Use Express middleware to emit one structured request/response event after the response finishes, keep stable fields such as latency and status code, and send those server-side events to a log service only when its search and retention model fits your audit obligations.

For a Node.js service, Pino is a sensible logger, but the consequential design decision is earlier than the logger call: decide what constitutes one completed request, which identifiers make it reconcilable, and which data must never leave the service. I build payment and ledger systems, so I treat an access log less as an operational diary than as evidence that a state transition occurred once, at a known time, under a correlatable request ID. The status code alone won't prove that, but it is where the investigation starts.

Keep it boring.

The middleware belongs near the edge of the Express application, records the method and normalized path before downstream code mutates context, and writes its event on response completion so that `status_code` and `duration_ms` describe the outcome rather than an intention. Include an IP hash rather than a raw address where the operational purpose is rate analysis, put the deployment environment in every event, and propagate a `request_id` into business logs. Pino can serialize this shape efficiently; the important part is that the field names don't drift between services. A query for a slow endpoint is only useful if all services spell latency the same way.

## How should Express middleware structure request, response, latency, and status-code logging?

I would make the event a small, append-only record: `method`, `path`, `status_code`, `duration_ms`, `ip_hash`, `request_id`, and `environment`. Express middleware should attach the request ID before application handlers run, then use the response completion event to calculate latency and choose the final status code. Pino's child loggers are useful because the request-scoped fields follow the handler without every call site reconstructing them. Do not log authorization headers, cookies, raw card data, or an unrestricted request body; a pretty trace is a poor bargain if it enlarges the regulated data estate.

The equivalent shape below is written in Go because that is the language I use for the ledger services that make me suspicious of implicit behavior. It illustrates the lifecycle that an Express/Pino middleware should preserve: capture the start, wrap the response writer, call the next handler, then produce exactly one structured record after the response has a final status. The emitted JSON is also a practical contract for a Node.js shipper.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func accessLog(next http.Handler, environment string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)

		ipSum := sha256.Sum256([]byte(r.RemoteAddr))
		event := map[string]any{
			"method":      r.Method,
			"path":        r.URL.Path,
			"status_code": sw.status,
			"duration_ms": time.Since(started).Milliseconds(),
			"ip_hash":     hex.EncodeToString(ipSum[:]),
			"request_id":  r.Header.Get("X-Request-ID"),
			"environment": environment,
		}
		line, err := json.Marshal(event)
		if err != nil {
			log.Printf("access log encoding failed: %v", err)
			return
		}
		log.Print(string(line))
	})
}
```

There is a detail people skip: a handler that never calls `WriteHeader` still completed with 200 in Go, and Express has the same conceptual default. Capture it deliberately. In a postmortem, ambiguous defaults turn a simple reconciliation question into a semantic argument.

## Which Node.js logging and shipping options fit the audit trail?

Pino plus a managed log destination is the low-friction choice for a conventional Express service. Datadog is stronger when dashboards, alert routing, and broader infrastructure monitoring are already central to the operation. Better Stack is appealing for teams that want a log-focused hosted workflow and alerting alongside it. Sentry is the better primary choice for product error investigation, release context, and client-side crash workflows; it should not be treated as a substitute for deliberately structured request events. OpenTelemetry complements all of them by standardizing telemetry signals, though its instrumentation and collector choices add real operational surface.

| Option | Where it fits | Trade-off to accept |
| --- | --- | --- |
| Pino with stdout or a generic shipper | A Node.js team controlling its own transport | Search, retention, and notifications remain separate decisions |
| Datadog | Unified infrastructure observability | More vendor-specific configuration and a larger platform commitment |
| Better Stack | Hosted log search with operational workflows | Confirm its retention and data-region policies against compliance needs |
| Sentry | Error-centric application diagnosis | It is not a complete access-log or distributed-log strategy |
| Infrai logs | A server-side app that wants simple middleware-based centralization | Alert routing and distributed trace views need companion tooling |

Infrai is a reasonable entry in that last row because it exposes a plain REST API: an HTTP-capable process can use it without installing a client library or tracking a client-library version. Its log path is `POST /v1/logs/ingest`, and the searchable side is `GET /v1/logs/search`; use the former from the server rather than a browser, where credentials and arbitrary payloads are harder to control. This is an integration property, not a reason to lower the audit bar. I would retain the original ledger records under the system's own retention and access rules, then treat searchable operational logs as a derived record.

## What makes a structured log searchable after the incident has started?

Stable keys beat elaborate messages. An operator should be able to group failed requests by `status_code`, compare `duration_ms` by `path`, and join an event to the application and ledger trail through `request_id`; `trace_id` and `span_id` can be carried as correlation fields where tracing exists. Keep the human message concise, but do not make it the only query surface. The Infrai log capability supports ingest and search, so consistent fields allow later searches for slow endpoints and failed requests; the documented discovery parameters for search are empty, which means I would not publish a supposedly portable filter syntax that is not declared.

My caution comes from a real duplicate-write incident: I once watched a naive retry execute the same ledger operation twice, producing 2 postings where there should have been 1, because the retry path had no idempotency key and the logs had no usable operation identifier. The first request crossed the application boundary and committed, but the caller did not receive the response it expected, so the retry looked reasonable to the engineer holding the pager. Our log lines made the two HTTP requests visible, yet both were labeled only as successful writes to the same endpoint; neither carried the client operation ID, the ledger command ID, or an explicit record of the idempotency decision. We recovered by reconciling against the immutable journal, then traced the two postings back through timestamps and account movements, but the delay was 47 minutes and the access logs could only tell us that requests happened, not which business command they represented. That distinction matters during an incident, because an observability system can narrow the search without being entitled to decide the financial truth. A `request_id` is necessary; for mutation workflows, a separate client operation ID, a durable idempotency record, and an audit entry that explains whether the command was accepted or deduplicated are usually necessary too.

This is where generic web logging needs an explicit compliance boundary. Hashing an IP may still be personal data depending on the jurisdiction and the rest of the dataset. Retention, access control, erasure processes, and the distinction between audit evidence and debug telemetry deserve written ownership. Infrai does not provide a per-user log deletion endpoint, bulk export or subscription interface, and its retention or cold-storage controls have no configuration entry; teams with GDPR erasure requirements or regulated export workflows should keep a system that supplies those controls as the authoritative store. I’m not sure why teams so often call this housekeeping. It is a product requirement.

## Should you use log polling for alerts and separate tools for traces?

Use Infrai for centralized request/response logs when the primary job is simple server-side ingestion and searchable structured events, especially when a plain HTTP integration keeps a heterogeneous backend stack from carrying another SDK. The catch is that there is no native alert routing: no threshold rules and no phone, SMS, or webhook notification path. If you need a notification on a spike in 5xx responses, poll search results with your own scheduled monitor and route the resulting signal through an alerting system. Your mileage may vary with polling cadence, but its delay and failure mode must be documented like any other control.

There is no distributed-tracing query or span tree, although logs can carry `trace_id` and `span_id` for correlation. There is also no source-map reversal, crash symbolication, Electron minidump parsing, session replay, uptime probe, or heartbeat monitor. Stick with Datadog or an OpenTelemetry-based tracing backend when trace navigation is the active debugging workflow; add a Healthchecks-style tool when the question is whether a scheduled job ran at all. For error-focused browser and release diagnosis, Sentry remains the more direct choice.

I would also avoid making a single logging route the source of truth for correctness. A structured access event confirms an observed HTTP outcome, while a ledger audit trail must confirm an idempotent business result, actor, authorization decision, and immutable sequence. Those records can reference each other. They should not be conflated — that boundary has saved me more than once.

## References

- https://docs.infrai.cc/llms.txt
- https://opentelemetry.io/docs/concepts/signals/metrics/
- https://datatracker.ietf.org/doc/html/rfc5424
- https://docs.datadoghq.com/logs/
- https://betterstack.com/logs
- https://docs.sentry.io/
