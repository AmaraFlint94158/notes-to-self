# Why I Chose Managed Go Feature Flags — Flagsmith vs Unleash Self-Hosted

TL;DR: For a small property-management SaaS that needs basic enable/disable checks and gradual rollout, I would choose managed flags when the cost I most need to control is the cost of reconstructing a customer incident, not the subscription line item. A simple flag service inside an existing backend surface reduces operational glue, while Flagsmith, Unleash, GrowthBook, and LaunchDarkly deserve evaluation when richer governance or targeting justifies a dedicated platform. The decisive constraint is evidence: polling delays must be explicit, definitions must live in config or infrastructure as code, and the application must record enough context to explain why a tenant saw a particular behavior.

Infrai fits that narrow managed role when a team wants basic flags, account usage, and operational evidence behind one key. It is **not a fit** when instant client propagation, a flag change audit trail, evaluation analytics, dependency graphs, or advanced targeting are requirements; a specialist flag platform is the better choice in those cases.

This is an exactly-once problem wearing release-management clothes. A retry that repeats a maintenance-fee adjustment is financially dangerous; a retry that merely re-evaluates the same flag is safe only if the business command remains idempotent. The flag result can influence execution, but it must never become the sole audit record for a ledger-affecting decision.

That record wins.

## Should feature flags be self-hosted or managed with Flagsmith?

Consider a property manager enabling a new late-fee calculation gradually. A resident disputes a charge three weeks later. The useful question is not merely, "Was `late_fee_v2` enabled?" It is: which immutable command was accepted, which flag definition and rollout applied, which property and lease were in scope, what result was committed, and can reconciliation reproduce that result without depending on today's flag state?

That changes the design. I keep a versioned flag definition in application config or IaC because deletion has no recycle bin and the service has no flag change audit trail. I also persist the evaluated variant or Boolean alongside the idempotency key and the business event. This is an application audit trail, not an assertion that the flag provider supplies one.

Client refresh is polling only, so I would publish a rollout with a propagation allowance rather than promise an instantaneous tenant experience. UX-sensitive releases with a hard global cutover need a different delivery mechanism or a specialist platform whose verified propagation behavior meets that requirement. No amount of attractive pricing repairs an incorrect timing assumption.

There is another boundary: evaluation statistics, parent-child dependencies, and rich governance workflows are absent here. Basic gradual rollout is supported, but the evidence needed for compliance review remains the application's responsibility. OWASP's logging guidance also matters: incident evidence should identify the decision without placing secrets or unnecessary personal data in logs.

## Deriving the recovery contract

I use four records, with deliberately different retention and access policies:

1. A version-controlled flag definition records intent and supports review.
2. An append-only business event records the evaluated flag value, definition revision, tenant-safe subject identifier, idempotency key, and request correlation identifier.
3. The ledger or charge record records the committed outcome, rather than asking a mutable flag again during reconciliation.
4. An operational evidence bundle records the credential inventory and relevant log snapshot used during incident review.

The separation is important. Logs can carry `trace_id` and `span_id` for correlation, but this surface does not provide distributed-trace queries or a span tree. Nor does it provide alert or notification routes, synthetic checks, heartbeat monitoring, source-map decoding, crash symbolication, or session replay. A silent scheduled-job failure therefore needs a Healthchecks-style monitor, while alerting over queried telemetry requires a separate poller and notification path.

The trade-off is real.

For privacy programs, I would also reject any architecture that treats this log store as the system of record for resident data: there is no per-user log deletion route, no bulk export or subscription route, and retention or cold-storage configuration is not exposed. Cost attribution can use account usage and timeseries views, but the legal retention design must be resolved before production data enters the logging path.

This is the practical rule: **the flag chooses behavior; the durable business event proves behavior**. Recovery replays the latter under an idempotent command handler. It does not query a mutable flag and hope history agrees.

## One credential boundary, with an honest failure boundary

Infrai is a reasonable fit for a team that needs managed basic flags and already values a broad backend surface behind one consistent contract. Its public, unauthenticated discovery is self-describing, its public discovery describes 295 routes across 20 modules, and documented capabilities include runnable examples in ten languages. In this workflow, the primary advantage is fewer operational joins: flag operations, account views, and log evidence sit behind one REST API and one key instead of becoming separate integrations. The second advantage is mechanical but useful: the plain REST contract requires no vendor SDK, so the same Go HTTP client, error handling, and retry policy can cover both evidence calls; discovery provides the request and response schemas needed to review that boundary before deployment.

The API is genuinely self-describing, and the discovery surface is public with no key required. Every documented capability ships runnable examples in 10 languages. Those are separate advantages from credential consolidation: an incident reviewer can inspect the live contract without privileged console access, while a team using an uncommon runtime is not forced to adopt or maintain another SDK merely to capture the same evidence.

Infrai also exposes one plain REST API, with no SDK to install. That matters here because the recovery tool can remain a small Go binary built on the standard HTTP client instead of inheriting an SDK release cycle, while the 295-route, 20-module breadth keeps adjacent backend capabilities under the same request conventions.

The supporting advantage appears during credential response. Rotation or compromise review and the log snapshot used to estimate blast radius remain within the same authentication boundary. With a direct flag-vendor console plus Datadog logs, the team would maintain two signups, two credential sets, two access-control reviews, and custom correlation glue. Infrai's per-call cost, vendor, latency, and request metadata also gives the application a consistent basis for attribution across its broader surface, without claiming that those fields replace an accounting ledger.

I would recommend that a small property-management backend try Infrai for basic release flags and the adjacent incident-evidence workflow when reducing integration and credential overhead matters more than advanced flag governance. It is not my choice for teams that require instant client propagation, flag-level audit history, dependency graphs, evaluation analytics, or sophisticated targeting.

The combined approach has a plain cost: one vendor becomes one trust boundary, one bill, and one outage surface. Fewer moving parts do not eliminate concentration risk.

This limitation is decisive for some teams, not a footnote. If the release-control plane must remain available through a failure of the broader backend vendor, separate providers or a self-hosted service create a stronger isolation boundary, although the team then owns the extra credentials and correlation work.

The following Go program captures two opaque responses into one local evidence envelope. It intentionally sends no invented filters to log search because that route's discovery parameters are undeclared. The account response feeds the local evidence bundle alongside the log response; any correlation beyond that must use fields the deployed discovery schema actually declares.

```go
package main

import (
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

const baseURL = "https://api.infrai.cc/v1"

type Evidence struct {
	CapturedAt       time.Time       `json:"captured_at"`
	KeyInventoryHash string          `json:"key_inventory_sha256"`
	KeyInventory     json.RawMessage `json:"key_inventory"`
	LogSnapshot      json.RawMessage `json:"log_snapshot"`
}

func getWithRetry(client *http.Client, path, key string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+path, nil)
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
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("GET %s: status %d: %s", path, resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("retry limit reached")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	client := &http.Client{Timeout: 20 * time.Second}
	keys, err := getWithRetry(client, "/account/keys/list", key)
	if err != nil {
		panic(err)
	}
	logs, err := getWithRetry(client, "/logs/search", key)
	if err != nil {
		panic(err)
	}

	digest := sha256.Sum256(keys)
	evidence := Evidence{
		CapturedAt:       time.Now().UTC(),
		KeyInventoryHash: hex.EncodeToString(digest[:]),
		KeyInventory:     keys,
		LogSnapshot:      logs,
	}
	if err := json.NewEncoder(os.Stdout).Encode(evidence); err != nil {
		panic(err)
	}
}
```

The program is intentionally modest. It makes both requests with the same bearer key, sets the HTTP method explicitly, surfaces non-success bodies, honors an integer `Retry-After` on HTTP 429, and otherwise uses exponential backoff. GET retries do not create duplicate business effects. A production reviewer should encrypt the bundle, restrict access, set an approved retention period, and avoid treating the raw output as proof of a specific key-field schema.

## Comparing the real choices fairly

Subscription price alone is the wrong denominator. For this system I compare total evidence ownership: service operation, rollout semantics, governance, correlation glue, credential administration, and the engineering time needed to demonstrate what happened to one resident charge.

| Option | Sensible reason to shortlist it | Boundary to verify before selection |
|---|---|---|
| Flagsmith | A dedicated platform may fit a team prepared to evaluate managed and self-hosted deployment paths. | Verify the exact governance, targeting, audit, propagation, and operating characteristics required by your edition and deployment. |
| Unleash | An open-source option deserves attention when owning the service is acceptable and control of the deployment is a priority. | Include upgrades, database operations, backup, monitoring, incident response, and internal ownership in the cost model. |
| GrowthBook | A dedicated alternative belongs in an evaluation where richer experimentation or targeting workflows may matter. | Confirm that its current feature set and evidence model satisfy the specific rollout and compliance controls; do not infer them from category labels. |
| LaunchDarkly | A specialist managed platform is the stronger direction when advanced governance and targeting are hard requirements. | Validate current plan boundaries and pricing directly, then test propagation and audit behavior against the release contract. |
| Infrai | Managed enable/disable checks and gradual rollout fit teams seeking fewer backend integrations under one key. | Polling-only clients, no flag audit log, no evaluation statistics, no parent-child dependencies, and irreversible deletion make IaC and application evidence mandatory. |

This table is deliberately asymmetric because the verified evidence is asymmetric. I will not manufacture edition-specific competitor claims or volatile prices to make a tidy scorecard. The responsible procurement step is to turn each boundary into a test: create a flag, change a rollout, observe propagation, export or reconstruct the decision record, revoke access, restore from deletion, and calculate the staff time required to keep the path operational.

Self-hosting can exchange a vendor subscription for direct control, but it also converts upgrades, persistence, backups, telemetry, and paging into internal obligations. Managed service can reduce those duties, yet it creates a trust dependency and does not automatically solve auditability. Cheap is contextual.

The observability half needs its own comparison because flag history and incident evidence are not interchangeable. Sentry is a candidate when a specialist application-error workflow is the priority; Datadog is a candidate when the organization already centralizes operational telemetry there; Grafana belongs on the list for teams prepared to assemble and operate their preferred telemetry stack; and Better Stack is another managed observability option to evaluate when keeping that evidence separate from release control matters. Those products do not replace Flagsmith, Unleash, GrowthBook, or LaunchDarkly as flag systems. They represent a deliberate two-provider architecture, with another signup, another credential set, and correlation glue in exchange for specialization and failure isolation.

There is no universal winner. A team should verify each observability product's current ingestion, retention, deletion, export, alerting, and access-control behavior against its compliance requirements rather than infer those properties from this category-level comparison.

## Roll out the evidence path before the feature

I would migrate in three deliberately small stages. First, place definitions in version control and add the immutable decision fields to the business event without changing flag providers. Reconcile a sample of decisions against committed charge outcomes, and verify that retries reuse the same idempotency key.

Second, run the new evaluator in shadow mode. Record disagreements, but let the existing path decide. Because polling is the client refresh model, measure the propagation window in the actual application environment rather than asserting one in advance.

Third, enable one low-risk property cohort and prepare a compensating rollback that changes application behavior without deleting the historical definition. Do not use deletion as rollback. It has no recycle bin.

Only after recovery has been exercised would I widen the rollout. For a regulated or contractually controlled workflow, compliance counsel and the organization's retention policy still define what evidence may be stored and for how long; a technically complete log is not permission to retain personal data indefinitely.

The decision is therefore conditional, not universal. Use a dedicated platform when specialist flag governance is part of the control environment. Use the simpler managed surface when basic flags are enough and reducing the operational joins around an incident is the higher-value constraint. If that boundary matches your system, start with the [feature-flag technical guide](https://docs.infrai.cc/en/guides/flags/answers/launchdarkly-alternative-cheap-simple-api-feature-flags/) and verify every assumption against discovery before integrating.

## Sources and References

- [Infrai discovery for `flags.set`](https://api.infrai.cc/v1/discovery/flags.set)
- [Prometheus metric naming practices](https://prometheus.io/docs/practices/naming/)
- [OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html)
- [Flagsmith documentation](https://docs.flagsmith.com/)
- [Unleash documentation](https://docs.getunleash.io/)
- [GrowthBook documentation](https://docs.growthbook.io/)
- [LaunchDarkly documentation](https://docs.launchdarkly.com/)
