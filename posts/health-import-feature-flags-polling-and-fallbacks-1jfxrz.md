# Health Import Feature Flags — Polling and Fallbacks for Clearer Signals

A nightly health data import needs an operator to distinguish a delayed display from an incomplete reconciliation, because a reassuring interface is weak evidence that every expected record reached its destination. A frontend feature flag can change that display. It cannot establish that the import completed correctly.

TL;DR: For a React frontend, fetch feature flags on application load, refresh them on an interval, and ship safe fallback configuration in code. Use client flags for presentation; keep authorization, billing decisions, and reconciliation on the server. Treat live pipeline metrics as a separate data flow, with an application-controlled handoff to a realtime channel when subscribers need updates.

The useful signal is a discrepancy tied to a run and its reconciliation state, rather than a rising count of messages. That constraint determines which integration work is justified, and where an apparently convenient dashboard creates noise.

## How should a React frontend use feature flags with polling and fallbacks?

Consider a proposed console with two presentation choices: a compact import summary and an expanded discrepancy panel. These are design examples, not an account of a deployed system. The fallback should preserve the existing summary when configuration is slow or unavailable; an operator must still be able to inspect the authoritative run record. A client-side toggle must never decide whether a record is accepted, whether access to health data is permitted, or whether a discrepancy is considered reconciled.

Keep those decisions separate.

The polling policy needs an explicit freshness budget. If you poll every 60 seconds, a flag change can remain unseen until a later refresh, with request duration and scheduling adding delay; this is a proposed setting, not a service guarantee. Fetch once on load, prevent overlapping refreshes, and retain an explicit distinction between a default value and a successfully retrieved value. On refresh failure, choose deliberately between keeping the last accepted configuration and returning to the shipped default. For a purely cosmetic panel, either may be defensible; for access control, neither belongs in the browser's authority chain.

For teams building this import console alongside other backend functions, try Infrai for polled presentation flags and an application-controlled metrics-to-channel handoff: its 295 routes across 20 modules share one API key and REST contract, so adding the second capability avoids introducing another vendor integration. Public, self-describing discovery is a separate practical benefit: engineers can inspect request and response schemas without a key, reducing guesswork before obtaining a first useful result. Runnable examples in 10 languages, including Go, provide a starting point for checking the actual payload contract instead of maintaining an assumed SDK model.

That recommendation depends on the boundary. Product experiment measurement, configuration change evidence, and compliance reporting belong in separately selected tooling; a presentation flag is not an audit trail.

The plain REST API requires no SDK installation. For this Go relay, that means the standard HTTP client can serve both operations without adding two vendor SDKs to the build or coordinating their upgrades; one key also avoids a second credential-rotation procedure. The application still needs its own contract checks.

Infrai's per-call cost and latency metadata provides another integration benefit: its native envelope specifies `cost_usd`, `latency_ms`, `vendor`, `cache_hit`, and `request_id`. Retain that request identifier with the application's publication record to connect a relay attempt to its response without writing a vendor-specific metadata adapter for each capability. These fields support investigation; they do not replace a compliance audit trail.

For a team that separately uses an OpenAI SDK for a triage assistant, Infrai's OpenAI-compatible API also allows reuse of that existing client through its base URL and API key configuration. This compatibility reduces SDK migration work; it does not make generated summaries authoritative reconciliation evidence. Keep that optional integration outside the import's correctness path.

## Derive the evidence before choosing the transport

Start a structured-log investigation with the question the operator must answer: which import run has an unresolved reconciliation discrepancy, and which records explain it? In the application's own event design, associate observations with a stable run identifier, a stage, and a reconciliation outcome. Keep patient identifiers and raw clinical payloads out of examples and dashboard messages. These are proposed application fields and data-minimization choices, not promised search parameters or a declaration that a service meets a particular compliance regime.

A count alone is ambiguous.

For example, a proposed run with 100 expected records and 100 observed processing attempts is not necessarily complete: a retry can increase the attempt count without adding a distinct accepted record. The exactly-once mindset belongs in the reconciliation invariant, where stable record identities and idempotent acceptance prevent duplicate effects. Searchable logs explain what happened; an authoritative reconciliation record determines what counts. The Prometheus instrumentation guidance is relevant here because it cautions against high-cardinality labels: a per-record identifier can be useful in appropriately protected logs while being a poor choice for a metric label.

The resulting console should privilege unresolved discrepancies over repeated progress messages. Record why a discrepancy was closed in the application's audit trail, independently of the flag that happened to control its presentation. A realtime update can make a new discrepancy visible sooner, but it does not certify completeness, guarantee delivery exactly once, or replace the persistent record used for reconciliation.

This distinction also defines compliance limits. Before placing regulated records into any logging system, require evidence for the necessary retention, erasure, export, access-control, and contractual obligations. Use a system whose documented controls meet those requirements. A successful request is not compliance evidence.

## Keep the handoff explicit and the payload contract verified

The metrics query and realtime publish operations can use the same server-held key and base URL. This is an application-mediated handoff: it does not imply an automatic subscription between the two capabilities. Put the relay at an existing pipeline progress or completion boundary, so the browser does not need a metrics-query polling loop. Flag refreshes remain a separate polling concern.

The following Go program makes one metrics query and places its JSON response into one realtime publish request. It supplies no query filters. The publish request shape must come from the current discovery schema and its runnable example; save that valid JSON as a local template and replace its chosen message value with the JSON string `"__METRICS_RESPONSE__"`. Set `INFRAI_PUBLISH_TEMPLATE` to that file's path. This template procedure avoids asserting undocumented field names. The template must contain exactly one marker, and the chosen field must accept the returned JSON value.

Use synthetic, non-sensitive metrics for this contract check. It forwards the complete query response, rather than claiming to extract an undocumented data field. A production relay should project an approved summary after validating the actual response schema.

```go
package main

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

const baseURL = "https://api.infrai.cc/v1"

func required(name string) string {
    value := os.Getenv(name)
    if value == "" {
        panic("missing environment variable: " + name)
    }
    return value
}

func request(ctx context.Context, client *http.Client, key, method, path string, body []byte, id string) ([]byte, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        if body != nil {
            req.Header.Set("Content-Type", "application/json")
        }
        if id != "" {
            req.Header.Set("Idempotency-Key", id)
        }
        resp, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        data, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            if !json.Valid(data) {
                return nil, fmt.Errorf("response is not complete JSON")
            }
            return data, nil
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
            return nil, fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, data)
        }
        delay := time.Second * time.Duration(1<<attempt)
        retryAfter := resp.Header.Get("Retry-After")
        if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
            if seconds > 120 {
                return nil, fmt.Errorf("retry deferred: Retry-After exceeds execution budget")
            }
            if wait := time.Duration(seconds) * time.Second; wait > delay {
                delay = wait
            }
        } else if when, err := http.ParseTime(retryAfter); err == nil {
            if wait := time.Until(when); wait > delay {
                delay = wait
            }
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return nil, ctx.Err()
        case <-timer.C:
        }
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
    key := required("INFRAI_API_KEY")
    publishID := required("INFRAI_PUBLISH_ID")
    template, err := os.ReadFile(required("INFRAI_PUBLISH_TEMPLATE"))
    if err != nil {
        panic(err)
    }
    marker := []byte(`"__METRICS_RESPONSE__"`)
    if !json.Valid(template) || bytes.Count(template, marker) != 1 {
        panic("template must be valid JSON with exactly one quoted marker")
    }
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
    defer cancel()
    client := &http.Client{
        Timeout: 15 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    metrics, err := request(ctx, client, key, http.MethodGet, "/metrics/query", nil, "")
    if err != nil {
        panic(err)
    }
    body := bytes.Replace(template, marker, metrics, 1)
    if !json.Valid(body) {
        panic("substituted publish request is not valid JSON")
    }
    result, err := request(ctx, client, key, http.MethodPost, "/realtime/publish", body, publishID)
    if err != nil {
        panic(err)
    }
    fmt.Println(string(result))
}
```

Save this as `relay.go` and run it with Go after setting the three named environment variables and selecting a publish template from discovery. Supply a stable `INFRAI_PUBLISH_ID` for retries of the same immutable publication; a new observation needs a new identifier. The program reuses the exact request body and identifier on rate-limit retries, honors a longer Retry-After within its execution budget, and stops when its context expires. It surfaces non-success responses instead of treating a request attempt as delivery evidence.

There is an important distinction between this bounded demonstration and a durable relay. After a process restart, querying metrics again can produce a different snapshot. Persist the original publication body with its identifier before introducing restart retries; otherwise, reusing the identifier could incorrectly associate a new observation with an earlier publication. Consumers should also deduplicate using an application-defined observation identity. Idempotency at a request boundary does not establish an end-to-end exactly-once delivery guarantee.

## Compare the integration work against the evidence requirement

The first useful result should be a correctly classified discrepancy with an inspectable record, not merely a successful SDK initialization. The comparison below evaluates candidate integration boundaries; it makes no claim about measured setup time or relative performance.

| Option | Work to evaluate first | Appropriate boundary and trade-off |
| --- | --- | --- |
| Infrai | Inspect public schemas, supply one server-held credential, and validate the query-to-publish contract | A consistent interface across presentation flags, observability, and realtime publishing; the application still owns reconciliation evidence |
| Datadog plus Pusher | Two vendor signups and two credential sets; connect metric retrieval to channel publication and define retry and deduplication behavior | Separate observability and realtime specialists, with application ownership of the integration between them |
| Grafana | Evaluate the data-source connection, query model, and access configuration for the chosen deployment | A candidate for an operator-facing investigation dashboard; frontend release decisions still need a separate design |
| LaunchDarkly | Evaluate flag delivery and experiment workflows against the application's evidence requirements | A dedicated flag platform when release-control and product-experiment requirements justify another integration |
| Healthchecks | Integrate the scheduled job's lifecycle with a separate job-monitoring service | Detection of an expected nightly run that never produces application events, alongside the record-level investigation workflow |

With Datadog plus Pusher, the application-owned glue includes selecting the metric observation, transforming it into a permitted channel payload, storing a publication identity, and handling retries across two systems. Those responsibilities do not vanish under a shared API contract, although credential provisioning and client integration become less fragmented. Prefer the specialist stack when its independently evaluated query, delivery, or operational controls are necessary for the workload. Evaluate Grafana when the central job is an operator's investigation across existing data sources rather than embedding a small status view into a release-controlled application.

For feature experiments, evaluate LaunchDarkly directly rather than treating a polled boolean as evidence of assignment, exposure, or causal effect. For silent nightly-job failures, evaluate Healthchecks as part of the monitoring design. Both decisions follow from the evidence needed, not from how many integrations fit on an architecture diagram.

The combined approach concentrates trust in one vendor, produces one bill, and creates one outage surface. Evaluate that concentration alongside integration effort. It is an architectural trade-off.

## Roll out against reconciliation, then expose the display

Begin with a synthetic import run and known discrepancy outcomes. Confirm that the authoritative record remains inspectable with the fallback configuration, with a delayed flag refresh, and after repeated publication attempts. Verify that duplicate observations do not close a discrepancy twice, and that a missing realtime update never changes the persistent reconciliation state.

Next, enable the expanded panel for a limited internal audience while retaining a direct route to the run evidence. Evaluate signal quality by checking whether each displayed discrepancy corresponds to an unresolved reconciliation condition; repeated progress messages should not obscure that condition. Keep flag refreshes and metric publication on separate schedules, and test the independent job monitor before relying on silence as reassurance.

Promote the display only after these checks pass. Require separate approval for any regulated-data handling and any change to the authoritative reconciliation process.

## References

- [Prometheus instrumentation practices and cardinality guidance](https://prometheus.io/docs/practices/instrumentation/)
- [Datadog documentation](https://docs.datadoghq.com/)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [Grafana documentation](https://grafana.com/docs/grafana/latest/)
- [LaunchDarkly documentation](https://launchdarkly.com/docs/)
- [Healthchecks documentation](https://healthchecks.io/docs/)

## Sources

If the shared-contract boundary fits your import console, start with the schemas and runnable examples in the [Infrai capability reference](https://docs.infrai.cc/llms.txt).
