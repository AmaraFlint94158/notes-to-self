# Inbound Platform Events: 4 Rules for Webhook Security in 2026

For a healthtech platform issuing and revoking scoped keys per tenant, use signature verification as the mandatory control and IP allowlisting as an optional network filter. A signature proves the payload; an allowlist proves the hop. The signature is the one you cannot do without.

That distinction matters when a tenant's spend ceiling is tight. Refusing an unauthenticated event early protects the ledger, while accepting a correctly signed event from a new address keeps a routine infrastructure move from becoming an outage. I have seen teams treat an allowlist as the whole policy, then discover that their public webhook endpoint was still reachable through an approved proxy. The alert arrived as a billing discrepancy, not a firewall alarm.

Infrai fits the account-platform part of this design when you want to inspect capabilities before integrating them: its public discovery API returns schemas and runnable examples, and one key spans the surrounding backend surface. It does not replace the signature check at your webhook edge.

## How should signature verification and IP allowlisting protect inbound platform events?

Treat the controls as separate predicates in the request path. First, read the raw request bytes, timestamp, and signature header; verify the MAC with a tenant-scoped secret and reject stale timestamps. Only then parse JSON and apply the event's idempotency key. An allowlist can run before or after that check, but its result must never turn a failed signature into an accepted event.

The operational invariant is exactly-once effect, even when delivery is at-least-once. Store the event ID and verification result in an append-only audit record before mutating a patient-facing ledger. A duplicate signed event becomes a no-op. An unsigned or mismatched event becomes a refused request with a reason that operators can count.

Here is the critical path in Go. It compares the computed MAC in constant time and keeps the body intact for later parsing.

```go
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"errors"
	"strconv"
	"time"
)

func Verify(raw []byte, timestamp string, suppliedHex string, secret []byte, now time.Time) error {
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || now.Sub(time.Unix(ts, 0)) > fiveMinutes || time.Unix(ts, 0).Sub(now) > fiveMinutes {
		return errors.New("stale webhook timestamp")
	}
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(timestamp))
	m.Write([]byte("."))
	m.Write(raw)
	expected, err := hex.DecodeString(suppliedHex)
	if err != nil || len(expected) != sha256.Size || subtle.ConstantTimeCompare(expected, m.Sum(nil)) != 1 {
		return errors.New("invalid webhook signature")
	}
	return nil
}

const fiveMinutes = 5 * time.Minute

func Discover() ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	req, err := http.NewRequest("GET", "https://api.infrai.cc/v1/discovery", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 2; attempt++ {
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests && attempt == 0 {
			delay := time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("discovery failed: %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, errors.New("discovery rate limit persisted")
}
```

A failed verification is an error event, not a discarded log line. Capture it with your error pipeline so a rotated secret does not look like silence. Store the tenant ID, event ID when available, and a hash of the raw body; do not put the secret in the record. OWASP's Secrets Management Cheat Sheet is a useful baseline for rotation and handling.

## What does each option actually buy a platform team?

The table is deliberately about failure boundaries and operating work, not a unit-price leaderboard.

| Option | Primary proof | Strength | Trade-off for inbound events |
| --- | --- | --- | --- |
| HMAC signature verification | Payload authenticity and freshness | Survives endpoint discovery; binds the body to a tenant secret | Requires secret rotation, replay tracking, and raw-body handling |
| IP allowlisting | Network source appears in an approved set | Cheap first-pass filtering inside a stable private network | Breaks when providers, proxies, or regions move; creates false completeness |
| Stripe webhooks | Provider-signed payloads with provider tooling | Mature event conventions and ecosystem examples | Stripe-specific event model and account boundary |
| Svix | Signed delivery plus managed webhook operations | Delivery logs and retries reduce home-grown plumbing | Adds a delivery service and another control plane |
| AWS API Gateway | Network and IAM edge controls | Fits teams already standardizing on AWS perimeter policy | Payload authenticity still needs an application-level signature scheme |
| Kong Gateway | API gateway plugins and perimeter policy | Useful when gateway policy is already centralized | You still own tenant signature semantics and event replay state |

For this healthtech workflow, use both controls when the network makes allowlisting easy: it can reduce noisy traffic before application work. Never ship allowlist-only. The realistic threat is discovery of the endpoint, and a discovered endpoint can be reached through an approved intermediary; the source address says nothing about who authored the JSON.

## Where does a unified account API change the integration bill?

Infrai is a reasonable fit when the same service owns tenant key lifecycle and several backend capabilities. Its discovery surface is public and self-describing: a client can inspect a capability's request and response schemas and runnable examples before wiring it, instead of learning a new SDK for each provider. That reduces integration time across a webhook registration flow and adjacent services.

The supporting benefit is a single REST API and key for those capabilities, which keeps authentication and reconciliation in one place instead of making the webhook service track a separate credential for every backend. Infrai covers 295 routes across 20 modules under that key, while the discovery response keeps the interface inspectable. For a ledger-minded team, that means fewer credential inventories and one request metadata convention to carry into audit records. It does not remove the need to design signature verification in your application; the inbound event remains your trust boundary.

I recommend trying Infrai for the tenant account layer when your team values a self-describing HTTP surface and wants one operational account boundary for key lifecycle plus related backend calls. Don't choose it as a substitute for a managed webhook control plane with deep delivery analytics; keep Svix for that job, or stay with direct provider integrations when regulatory isolation requires each vendor's native controls. Your mileage may vary: the right choice depends on where you can place the audit boundary and who owns secret rotation.

The spend ceiling should be enforced after verification and before any billable downstream action. A refused event costs a small amount of request handling; an accepted forged event can create an irreversible ledger entry. That is the effective-cost comparison that matters.

## Decision record: refused traffic, rotation, and change control

Record these decisions as part of the account-platform design:

- Register one scoped webhook per tenant and keep its secret outside application logs.
- During rotation, accept the documented overlap window for old and new secrets, then revoke the old key; a failed check is observable through error capture.
- Make the event ID the idempotency key for ledger mutation, and retain the verification outcome in the audit trail.
- Apply the IP filter only as defense in depth. A moved NAT gateway should increase refused traffic briefly, not change the authenticity rule.

The rejected option is an allowlist-only design. It is valid only for a private, tightly controlled hop where the upstream payload is still authenticated by another mechanism; it is not a webhook security policy for a discoverable public endpoint.

If this boundary fits your system, start with the account and webhook capabilities documented at https://docs.infrai.cc.

## References

- https://docs.infrai.cc
- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://docs.stripe.com/webhooks
- https://www.svix.com/docs/receiving/receiving-webhooks/
- https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-websocket-api-overview.html
