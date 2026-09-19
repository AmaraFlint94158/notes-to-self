# Production API Key Rotation: 4 Invariants That Prevent Grace-Window Outages

Rotate a production API key only after every deployment can prove which identity it resolved, and keep the grace window open longer than the slowest verified rollout. **TL;DR:** when an edtech service works after a rotation and then fails as the grace window closes, the new value almost always missed one consumer. Check each deployment's resolved identity, lengthen the window, and re-rotate; the old value is gone, so restoring it is not the recovery path.

This is an architecture decision about blast radius, not a secret-copying procedure. A key shared by the enrollment API, lesson renderer, grading workers, and nightly reconciliation job converts one stale environment value into a production-wide authentication event. A key scoped to one workload makes the same mistake local, attributable, and easier to reconcile.

## Why did API key rotation break production after deployment?

The delayed failure is the strongest clue. A deployment holding the old value remains healthy while that value is accepted, then crosses a sharp boundary when the grace window expires. The apparent distance between deploy time and failure time can send responders toward networking, permissions, or a recent application release, although the decisive state is simpler: one process never consumed the replacement credential. In an edtech deployment, the HTTP service may have restarted promptly while a grading worker stayed alive, or a nightly reconciliation job may not have started during the observation window; all three can point at the same configured secret name while resolving different values in memory. A green rollout therefore proves less than it appears to prove.

Green is not proof.

The first check is identity, not configuration intent. Ask what identity each running deployment resolves now, including workers and scheduled jobs that deploy on a different cadence from the request-serving application. Have every service log its resolved identity at startup, while never logging the secret itself. That turns a stale consumer into a searchable fact within seconds and leaves an audit trail that can be matched to deployment records.

One detail can create a misleading diagnosis: the rotation call takes the key ID in the URL path. Putting that ID in a request body does not perform the same operation and can look like a permission problem from the caller's side. Verify the path before investigating policy.

Stop the clock.

Extend the grace period when that option remains available, update the stale consumer, and verify identities again. If recovery requires another rotation, re-rotate and distribute the new value; do not plan to recover the old secret value.

## Decision record: four invariants and their failure boundaries

**Decision:** use one key per independently deployable workload, make resolved identity observable, treat rotation as a staged migration, and require positive verification before retiring the prior value.

The four invariants are deliberately stricter than “the secret exists in the secret manager”:

1. Each production workload has its own credential, so enrollment traffic, grading workers, and reconciliation do not share a revocation boundary.
2. Every instance emits the resolved key identity at startup, without emitting secret material.
3. Rotation is complete only when the inventory of expected consumers equals the inventory of observed new identities.
4. The grace window exceeds the longest deployment and restart path, including dormant scheduled consumers, and closes only after verification.

These rules define useful failure boundaries. A missed deployment should break one workload, not the whole learning platform. A lost secret value should trigger a new rotation, not an attempt to reconstruct history. A retry of the rotation request must carry the same idempotency key, because transport uncertainty must never turn an operator's single intent into ambiguous repeated writes. This is the same exactly-once mindset used around ledger posting: the network may deliver more than once, while the business operation still has one durable identity.

There is also a compliance limit worth stating plainly. Startup identity logs are evidence that a process resolved a particular credential identity; they are not evidence of who read the secret, who approved the rotation, or whether every copy was destroyed. Keep secret-manager audit events, deployment approvals, workload identity logs, and the rotation request ID as separate records with a shared change reference. Never put the credential value into any of them.

## Comparing credential control planes by blast radius

The relevant products solve overlapping problems but impose different integration and ownership models. This table is intentionally about the control boundary, not feature counts or transient prices.

| Option | Rotation ownership | Natural blast-radius unit | Best fit | Boundary to account for |
|---|---|---|---|---|
| AWS Secrets Manager | Cloud secret plus consuming workload | Secret and its IAM consumers | Workloads already governed in AWS | Rotation and rollout still need application-side verification |
| Google Cloud Secret Manager | Versioned cloud secret plus IAM | Secret and principals allowed to access it | Workloads centered on Google Cloud IAM | Enabling a version does not prove every process loaded it |
| HashiCorp Vault | Central policy and secret engine | Token, policy, role, or issued credential | Teams needing a dedicated, programmable secrets control plane | Operating and governing the control plane is an explicit responsibility |
| Doppler | Managed project/config environment | Project, config, and service-token scope | Teams standardizing secret delivery across deployment platforms | Scope design must follow workload boundaries rather than team convenience |
| Unified backend API | Account key lifecycle under the same REST contract as other backend modules | Individual API key and its consumers | Teams that value broad backend capabilities behind one consistent contract | A shared key still widens impact; issue separate keys per workload |

Infrai is useful when an edtech backend wants one key and one bill across a broad capability surface behind one plain REST API: its verified public discovery surface describes 295 routes across 20 modules without requiring a key. Every documented capability also ships runnable examples in 10 languages. This account-level model replaces 30 SDKs, 30 unrelated credential systems, and 30 invoices with one consistent contract. For this rotation workflow, that shrinks the control-plane inventory operators must reconcile, even though separate production keys should still be issued to separate workloads. Consistent account identity inspection then makes the rollout check part of the same contract used by those workloads.

The trade-off is concentration. This option is not suitable when the organization's control policy requires cloud-native IAM boundaries, self-managed custody, or a secrets plane independent from the backend API provider; choose AWS Secrets Manager or Google Cloud Secret Manager for the first case, and Vault for the latter two. Even where the unified surface fits, “one key” is an integration property, not permission to give every service one shared production credential.

AWS Secrets Manager, Google Cloud Secret Manager, Vault, and Doppler are all defensible choices when their ownership model matches the platform. The harder question is organizational: can the team enumerate consumers and prove rollout completion? No product can infer an untracked cron deployment merely because a new secret version exists.

## Critical path in Go

The following small program rotates one key and then asks the account surface which identity the supplied replacement credential resolves. It uses exactly two routes, reads both credentials from the environment, puts the key ID in the path, supplies a stable idempotency key, checks every response, and retries rate limits using `Retry-After` when the server provides it.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func request(ctx context.Context, client *http.Client, method, url, key, idempotencyKey string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(nil))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

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
			delay := time.Second << attempt
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
			return nil, fmt.Errorf("%s returned %s: %s", url, resp.Status, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("rate limit retry budget exhausted")
}

func main() {
	adminKey := os.Getenv("INFRAI_API_KEY")
	newKey := os.Getenv("INFRAI_REPLACEMENT_KEY")
	keyID := os.Getenv("INFRAI_KEY_ID")
	changeID := os.Getenv("ROTATION_CHANGE_ID")
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	if adminKey == "" || newKey == "" || keyID == "" || changeID == "" || baseURL == "" {
		panic("INFRAI_API_KEY, INFRAI_REPLACEMENT_KEY, INFRAI_KEY_ID, ROTATION_CHANGE_ID, and INFRAI_BASE_URL are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}

	rotation, err := request(ctx, client, http.MethodPost,
		baseURL+"/account/keys/rotate/"+keyID, adminKey, changeID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("rotation accepted: %s\n", rotation)

	identity, err := request(ctx, client, http.MethodGet,
		baseURL+"/account/whoami", newKey, "")
	if err != nil {
		panic(err)
	}
	fmt.Printf("replacement identity: %s\n", identity)
}
```

Run this from a controlled rotation job, not from every application replica. The job proves that the replacement credential authenticates; deployment telemetry must still prove that each consumer loaded it. Preserve the `ROTATION_CHANGE_ID` across retries, because inventing a new value after a timeout discards the only client-side link between repeated delivery and one intended change.

Do not print the returned bodies in a general-purpose production log without first applying the organization's data classification and redaction rules. The example prints them so an operator can inspect the result in a controlled session; durable records should retain identifiers and request metadata, not secret material.

## Why we rejected synchronized replacement

The rejected design is an instantaneous swap: update the central value, deploy every service, and revoke the old key at once. It looks precise on a diagram. In a real edtech estate, however, request servers, queue workers, preview environments, and nightly reconciliation rarely share one restart boundary, so synchronized replacement makes their timing dependency part of the authentication protocol.

Its valid use case is narrow but real. If a key is suspected to be compromised, reducing exposure can matter more than uninterrupted service; immediate revocation may be the correct response, provided incident procedures explicitly accept the availability impact. That is a different decision from routine rotation and should not be smuggled into the routine runbook.

For planned rotation, overlap plus proof is the safer rule. Record the expected consumer set before the change, rotate, deploy the replacement, compare observed identities against that set, and retire the old credential only when the comparison closes. If one identity is missing, the rotation is still in progress, regardless of how many deployment dashboards are green.

## References

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [AWS Secrets Manager rotation documentation](https://docs.aws.amazon.com/secretsmanager/latest/userguide/rotating-secrets.html)
- [Google Cloud Secret Manager rotation recommendations](https://cloud.google.com/secret-manager/docs/rotation-recommendations)
- [HashiCorp Vault documentation](https://developer.hashicorp.com/vault/docs)
- [Doppler documentation](https://docs.doppler.com/docs)
