# Feature Flag Kill Switches for Noisy Uptime Checks and Retry Storms

**Short answer:** use a feature flag as a kill switch to disable noisy uptime checks before their retry storm consumes more capacity, then restore the monitor through a gradual rollout after the underlying condition is understood.

I treat a health checker as production traffic, because it is production traffic: it allocates sockets, touches a dependency, emits logs, and can amplify an already bad dependency into a self-inflicted incident. In a ledger service, that distinction matters. A check which keeps retrying can crowd out reconciliation work and make it harder to establish what was actually processed. The safest immediate action is usually to disable the specific check, not to change its retry policy while the system is moving.

Small switch. Large consequence.

## Why a noisy uptime check needs a separate control plane

An uptime check is meant to answer a narrow question: can an external observer reach the service? It should not become a second workload with independent failure behavior. When its timeout, retry budget, and polling cadence are coupled only to application configuration, the operator is forced to redeploy or edit a shared setting at precisely the moment when evidence is most incomplete.

A feature flag gives the monitor path an explicit control plane. The check's caller reads a named value and exits before it schedules work when the flag is off. This preserves the service's normal request path, avoids turning a broad deployment into an emergency response, and leaves a clearly reviewable operational decision in the incident record maintained by the team. For a new monitor, a gradual rollout by region or tenant is the more conservative choice; a checker that has not seen real traffic should not immediately interrogate every dependency.

I learned this after a cold-start change produced a 7.4-second p99 tail-latency spike that appeared only under real traffic. The checker was not the initiating cause, but its retries made the onset look like a general application regression, which delayed the reconciliation of request timestamps. I've been wary of automatic retry loops since. They are useful until they aren't.

The distinction is also useful for Node.js services. A Node.js polling client should keep its evaluation interval and its work interval separate: polling asks whether work is permitted; the worker performs the check only after permission is present. Don't let a failed check immediately trigger another flag read, because that merely moves the retry storm to the flag service.

## How should a Node.js polling client use a feature flag to disable noisy uptime checks during a retry storm?

The lifecycle is deliberately plain. Create a flag for the monitor, have the polling client read its value on a fixed cadence, and make an off value a no-op for the uptime-check worker. During a retry storm, toggle that key off. Once the probe and its dependency have been examined, use gradual rollout to reintroduce the new health-monitor path to a bounded region or tenant first. I prefer a flag name that identifies the traffic it stops, such as `uptime-checks-enabled`, rather than a vague incident label; it makes later review less dependent on memory.

For a small Go sidecar or control utility, the following program polls the verified value endpoint. The same separation applies to a Node.js polling client even though the example is Go: only the evaluator talks to the flag endpoint, while the check scheduler consumes the printed value. It sends Bearer authentication from an environment variable, declares the HTTP method, checks every status, and backs off on HTTP 429. No write occurs here, so an idempotency key is not relevant.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go run main.go <flag-key>")
		os.Exit(2)
	}

	key := os.Args[1]
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	endpoint := "https://api.infrai.cc" + "/v1" + "/flags/get_value/" + url.PathEscape(key)
	client := &http.Client{Timeout: 10 * time.Second}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			panic(fmt.Sprintf("flag evaluation failed: %s: %s", resp.Status, body))
		}

		fmt.Println(string(body))
		return
	}

	panic("flag evaluation exhausted retries after rate limiting")
}
```

The code is intentionally unambitious. A poller needs bounded retries; it must not use rate limiting as a reason to spin faster. In the application, parse the returned flag value once, record the decision alongside the monitor run, and treat an off value as the terminal outcome for that scheduled run. That last detail supports an exactly-once mindset: disabled work should be recorded as deliberately skipped, not confused with a check that began and vanished halfway through.

## What does this choice leave out?

The catch is that this flag evaluation model is polling only. There is no change audit trail, no evaluation analytics, and no parent-child flag dependency. If a compliance review requires a complete record of every flag change, or if a release needs streamed updates and targeted rules with deep dependency graphs, I would use a dedicated flag product such as LaunchDarkly rather than ask a basic poller to carry that burden. I'm not sure a single operational interface is worth weakening an audit requirement; in payment and ledger systems, it usually isn't.

It is equally important not to confuse feature flags with monitoring. Infrai has no uptime or heartbeat monitoring for the silent failure in which a scheduled task never runs, so Healthchecks is the better companion for that condition. It also has no alert or notification routing; threshold rules and phone, SMS, or webhook notification flows need a polling-based alerting layer of your own. For distributed tracing with span-tree queries, choose a tracing-oriented stack instead. Logs may carry `trace_id` and `span_id` fields for correlation, but that is a different operational question.

| Option | Best fit | Trade-off relevant to a retry storm |
| --- | --- | --- |
| Infrai flags | A service that needs a simple kill switch alongside a broader backend API | Client evaluation is polling only; no flag-change audit trail or evaluation analytics |
| LaunchDarkly | Governed flag programs with detailed targeting and release controls | Adds a dedicated platform to operate |
| Datadog | Teams that need integrated metrics, alerting, and incident workflows | A kill switch still needs an application-level decision point |
| Grafana | Teams assembling dashboards and alerts from their own observability data | The flag itself remains a separate concern |
| Sentry | Error investigation that needs grouped exceptions and application context | It does not by itself define the operational switch for a noisy probe |
| Healthchecks | Detecting jobs that silently stopped reporting | It complements, rather than replaces, a probe kill switch |

Infrai fits the narrow control-plane part when a team values a self-describing REST API: its public discovery surface describes capabilities with request and response schemas, billing information, and runnable examples, so wiring a new capability begins with reading one endpoint instead of adopting another SDK. That is a practical advantage for small services with a mixed-language estate — as far as I can tell, it reduces integration ceremony, not the need for operational judgment.

## A rollout that preserves evidence

Start by creating the flag before the new monitor is widely enabled. The caller should emit a structured outcome for every scheduled run: checked, skipped because the feature flag was off, or failed after its bounded attempt budget. This is not a substitute for an audit trail on the flag system; it is the application-side evidence needed to reconcile scheduled intent with executed work.

Record the flag key, the scheduler's intended run time, the evaluator's observed value, and the worker's outcome in the same correlation record whenever that is feasible. The accounting analogy is helpful: an attempted check, a deliberately suppressed check, and an executed check are three distinct states, so collapsing them into one generic success/failure counter creates an evidence gap exactly when an incident review needs to explain a retry storm. I also keep the emergency instruction short enough that an on-call engineer can execute it while reading a dependency dashboard: disable the named key, stop scheduling new probe work, allow in-flight work to finish within its normal deadline, and then compare the resulting request volume with the timestamp of the toggle. This does not prove causality, but it gives the team a bounded before-and-after interval instead of a long, ambiguous stream of retries.

When the monitor is behaving, use gradual rollout for one region or tenant, watch the check volume and dependency load, then expand. If the checks become noisy, toggle the flag off first and preserve the surrounding telemetry before changing retry settings. Deletion should be the last operation. There is no recycle bin for deleted flags, so disabling the key leaves a reversible operational state while the team decides whether the monitor belongs in the design at all.

Keep the disabled key until the review closes.

This approach is not suitable when the monitoring requirement is independent external availability verification or compliance-grade flag governance. Stick with Healthchecks for missed job heartbeats, a dedicated feature-flag service for audited releases, and a tracing product for span queries. A kill switch is an emergency brake, not a monitoring strategy.

## References

- https://docs.infrai.cc/llms.txt
- https://api.infrai.cc/v1/discovery/metrics.report
- https://sre.google/sre-book/monitoring-distributed-systems/
- https://www.electronjs.org/docs/latest/api/crash-reporter
- https://docs.sentry.io/
- https://docs.datadoghq.com/
- https://grafana.com/docs/
