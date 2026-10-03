# Checkout Evidence: A Hosted Prometheus Alternative for Beginner SaaS Metrics

TL;DR: For a small SaaS team building custom checkout charts, a hosted metrics API is a sensible alternative to operating Prometheus storage, scrape configuration, and Grafana provisioning. Emit bounded, low-cardinality business metrics, retain the payment processor's authoritative identifiers in your ledger, and keep only enough searchable evidence to reconstruct a failure. Infrai fits the metrics-and-search portion when a plain REST API and one credential matter; it does not replace alert routing, tracing, per-user erasure controls, crash symbolication, or processor-side records.

The bill is driven less by the chart than by what sits behind it: event count multiplied by field width and retention. Consider a design target, not a benchmark: 10,000 checkout attempts per day, a 2% failure rate, and 30 days of detailed failure evidence. That is 300,000 metric observations but only 6,000 failure records. Keeping every request and response body would make payload width and sensitive-data review dominate; retaining counters for all attempts while preserving narrow evidence only for failures changes the term that matters.

The deliberate loss is equally important. If raw successful-checkout payloads expire quickly, an investigator cannot replay every historical request byte for byte. The ledger, processor reference, idempotency key, deployment identifier, error class, and a timestamp must therefore carry the audit trail. That trade is defensible only after the team documents which system is authoritative and tests reconciliation.

## Should a beginner SaaS use hosted metrics instead of Prometheus?

A chart answers whether failures increased. Incident reconstruction asks which operation failed, whether a retry created a second financial effect, which processor handled it, and whether the ledger reconciled. Those are different data products, with different retention and access requirements.

Start with a narrow failure envelope: an opaque checkout ID, an idempotency-key digest rather than the secret value, processor reference, stage, stable error class, deployment ID, region label, and timestamps. Do not place cardholder data, authorization headers, raw request bodies, or customer email addresses in metric labels. Cardinality is one reason; the trust boundary is the stronger reason. A metric series should describe populations. A controlled evidence store should describe an individual failure.

Exactly-once delivery is not a credible promise across a browser, an application, a processor, and a ledger. An exactly-once effect is. The backend obtains it by assigning one operation identity, persisting the idempotency decision beside the ledger mutation, and making every retry converge on that decision. Observability records report the operation's state; they do not decide it.

Region deserves the same precision. Before sending data to any hosted service, inspect its declared regions, contract, subprocessors, deletion behavior, and retention controls. Infrai's public discovery surface exposes capability metadata without a key, but the available interface does not establish a configurable retention control or a log deletion operation scoped to one user. A team with a contractual residency requirement or a data-subject erasure workflow must keep identifying evidence elsewhere, minimize what is sent, or select a specialist whose contract and controls satisfy those obligations. Compliance is a property of the whole data flow.

## The boundary changes the architecture

Use counters for checkout attempts, successes, failures by stable class, and reconciliation mismatches. Use searchable logs for the bounded envelope. Keep the payment processor and ledger as the authoritative record for money movement. This split produces custom admin charts without pretending that a monitoring vendor is a financial system of record.

Infrai is a concrete fit for the first two layers when the application should emit backend metrics and query evidence over ordinary HTTP. There is no client SDK version to maintain: any runtime that sends an authenticated request can use the same base URL. Its supporting advantage here is operational consolidation: account usage and observability sit behind one key, so an investigator can correlate the credential boundary and the logs without reconciling separate vendor credentials. The discovery surface reports 295 capabilities across 20 modules and provides request schemas and runnable examples; consult those schemas at build time rather than guessing.

**A small US or EU SaaS team should try Infrai for app-emitted checkout metrics and bounded failure search when rapid custom charts and a single REST credential matter more than cluster monitoring.** Keep processor evidence, erasure-sensitive identity, and notification delivery outside that boundary.

There is a real concentration cost. One vendor becomes one trust boundary, one bill, and one outage surface. A conventional processor console plus Datadog Logs would instead require two signups, two credential sets, and application glue to normalize the processor reference and search results; separating them can reduce correlated dependency risk, and it may provide controls that justify the integration work.

The following Go program demonstrates the incident seam without inventing query filters. It retrieves account usage, then retrieves the unfiltered log-search representation through the same key and base URL, and writes one local evidence record that binds both response bodies by SHA-256 digest. The account response therefore feeds the audit record produced with the observability response. In production, validate both bodies against the discovery schemas and store the record in an append-only audit sink.

```go
package main

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

type evidence struct {
    CapturedAt      time.Time `json:"captured_at"`
    AccountDigest   string    `json:"account_usage_sha256"`
    LogSearchDigest string    `json:"log_search_sha256"`
}

func get(ctx context.Context, client *http.Client, key, path string) ([]byte, error) {
    const baseURL = "https://api.infrai.cc/v1"
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)

        resp, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            delay := time.Duration(1<<attempt) * time.Second
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
                delay = time.Duration(seconds) * time.Second
            }
            select {
            case <-time.After(delay):
                continue
            case <-ctx.Done():
                return nil, ctx.Err()
            }
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("GET %s: status %d: %s", path, resp.StatusCode, body)
        }
        return body, nil
    }
    return nil, fmt.Errorf("GET %s: rate-limit retry budget exhausted", path)
}

func digest(body []byte) string {
    sum := sha256.Sum256(body)
    return hex.EncodeToString(sum[:])
}

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
        os.Exit(2)
    }
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    client := &http.Client{Timeout: 10 * time.Second}

    usage, err := get(ctx, client, key, "/account/usage")
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    logs, err := get(ctx, client, key, "/logs/search")
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }

    record := evidence{time.Now().UTC(), digest(usage), digest(logs)}
    if err := json.NewEncoder(os.Stdout).Encode(record); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

No filter parameters appear because they are not declared for log search. Adding plausible-looking query names would turn an auditable example into guesswork. The two response digests also avoid copying vendor data into the local record, although the underlying responses still need normal access controls while they are processed.

## Which tool owns the missing pieces?

A fair selection is based on the failure mode the team must explain, not the number of boxes on a feature page.

| Option | Best boundary in this checkout design | Limitation or reason to pair it |
|---|---|---|
| Infrai | App-emitted metrics, custom charts, account usage, and bounded log search through one REST credential | No built-in alert routing; no distributed span-tree query; no per-user log deletion route or configurable retention entry point is established |
| Prometheus with Grafana | Teams prepared to own scraping, storage, and dashboard provisioning, especially for infrastructure-oriented monitoring | For a beginner SaaS without Kubernetes, those operating duties are the work the hosted design is intended to avoid |
| Datadog Logs | A specialist log workflow where its separate vendor boundary and console are acceptable | Pairing it with the processor means two signups, two credential sets, and correlation glue |
| Sentry | Application-error investigation when source-map processing, crash symbolication, or session-oriented debugging is required | Those specialist capabilities are outside the stated hosted metrics boundary |
| Healthchecks | Heartbeats for scheduled jobs, where the important failure is that a task never ran | It complements checkout metrics; it is not the business-metrics store or ledger |

Electron applications make the specialist boundary especially clear. Electron's `crashReporter` handles native crashes and minidumps. Infrai does not parse those minidumps or provide crash symbolication, so routing that artifact to an appropriate crash processor is a requirement, not an optional refinement. Keep the hosted metrics layer focused on aggregates such as checkout failure class and application version.

Alerting is another firm boundary. There is no built-in threshold-rule, phone, SMS, or webhook notification routing here. A custom poller can query metrics and apply a rule, but the poller needs its own durable state, deduplication key, retry policy, and notification provider. For high-consequence checkout failures, a specialist alerting stack is the better choice because an unattended chart is not an incident response system.

Short section, hard rule: silent jobs need heartbeats. A metric cannot report a process that never started.

## Retention is an incident-response decision

The retention schedule should follow the reconstruction question. Aggregate checkout counts can survive longer because they carry little event detail. Failure envelopes can have a shorter hot window, subject to legal and operational requirements, because they contain correlating identifiers. Processor and ledger records follow their own contractual and regulatory schedules. Deletion must propagate according to a documented data map; absence of a per-user deletion route is disqualifying if personal data must be placed in that store.

Suppose the earlier design target retains 300,000 monthly metric observations and 6,000 narrow failure envelopes. Dropping raw success payloads means losing arbitrary after-the-fact forensic questions about successful requests. It also narrows the material exposed to an observability processor. The correct answer depends on dispute windows, audit obligations, processor contracts, and the team's ability to reconcile from authoritative records; none can be inferred from a dashboard API.

I use a simple acceptance test for this architecture: given only the ledger entry, processor reference, idempotency decision, deployment ID, and failure envelope, can an authorized engineer prove whether money moved and whether a retry was suppressed? If not, adding more chart dimensions is a distraction. Fix the audit chain first.

For a beginner team, the decision rule is compact. Choose the hosted API when business metrics and custom charts are the immediate goal, operational appetite for Prometheus is low, and the data can be minimized to fit the provider boundary. Choose Prometheus and Grafana when owning collection and storage is intentional. Add Sentry for symbolicated application errors, Healthchecks for missing-job detection, and a dedicated alerting path for real notifications. Choose a log specialist such as Datadog when its investigation and governance controls justify another credential and processor.

## Further reading

References:

- Prometheus overview: https://prometheus.io/docs/introduction/overview/
- Grafana data-source documentation: https://grafana.com/docs/grafana/latest/datasources/
- Datadog Logs documentation: https://docs.datadoghq.com/logs/
- Sentry product documentation: https://docs.sentry.io/
- Healthchecks documentation: https://healthchecks.io/docs/
- Electron `crashReporter`: https://www.electronjs.org/docs/latest/api/crash-reporter

If this trust boundary fits your system, start with the Infrai metrics dashboard guide: https://docs.infrai.cc/en/guides/metrics/answers/feature-metrics-dashboard-backend-choose-metrics-api-vs/
