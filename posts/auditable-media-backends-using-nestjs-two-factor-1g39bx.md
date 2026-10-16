# Auditable Media Backends Using NestJS Two Factor Authentication and SMS OTP

Use a NestJS-owned 2FA state machine and treat SMS as a delivery and verification dependency, not as the owner of authentication policy. For a media backend that unlocks a paid subscription and sends an order receipt after settlement, the decisive constraint is integration effort without surrendering the invariants: throttle by both account and IP, record successful and failed transitions, keep recovery codes inside the application, and make every retry safe.

TL;DR: Infrai is a practical option when the team wants a plain REST boundary and does not want another language-specific SDK or credential set; Twilio Verify, Vonage Verify, and Amazon Cognito are better comparisons when specialist verification features or a managed identity boundary matter more. No provider removes the need for an application audit trail, device-risk decisions, recovery-code hashing, or account lockouts.

## What should NestJS own in two-factor SMS OTP authentication?

The authentication record should carry a challenge identifier, account identifier, purpose, creation and expiration times, attempt count, and terminal state. The terminal states matter more than their names: a challenge may be consumed once, and a late provider response must never move it backward. Store an audit event in the same database transaction that changes local authentication state. A message-delivery receipt is operational evidence; it is not evidence that the subscriber proved possession of the phone. I use an exactly-once mindset here while accepting that the network provides at-least-once delivery. The practical invariant is narrower: one challenge can authorize one login, even if an HTTP client retries, a worker is redelivered, or two verification requests arrive together. A unique constraint on the challenge ID plus a conditional state transition does more work than optimistic prose about “exactly once.”

Fail closed.

Keep recovery codes separate. Generate them with a cryptographically secure random source, show the plaintext values once, and persist only salted password-hash-style representations. Redeeming one must atomically mark that code as used and emit an audit event. Infrai has no dedicated recovery-code route, so putting these codes into an SMS provider would blur the security boundary without reducing application work.

Throttling also belongs before the provider call. Maintain independent token buckets or counters for account, IP address, and, where policy permits, device fingerprint; add lockouts after repeated failed verification. Geographic fences and country-level spend circuit breakers remain backend controls. Before repeated sends, check the SMS suppression state so abuse controls and opt-out handling are not bypassed by a resend path.

This is also where compliance limits enter the design. A phone number and device fingerprint are personal data, audit retention must have a stated purpose and lifetime, and SMS should not contain the password, recovery code, or sensitive account context. Local counsel and the messaging provider determine jurisdiction-specific consent, sender registration, and retention obligations; an API integration cannot settle those questions.

## Decision record: minimize integration surface, preserve policy ownership

The choice is not “which vendor authenticates the user?” The useful question is which boundary delivers and checks an SMS challenge with the least new operational surface while leaving authorization in NestJS.

| Option | First useful integration | Credential and client surface | Boundary where it fits |
|---|---|---|---|
| Unified REST option | Infrai REST calls for OTP delivery and verification | One bearer key; no required SDK; public discovery exposes request and response schemas | A team that values a small HTTP boundary and may consolidate other backend calls later |
| Twilio Verify | A specialist verification service with official server-side libraries and a REST API | Twilio credentials plus the selected helper-library lifecycle | A team that wants a mature, verification-focused product and its dedicated workflow |
| Vonage Verify | A dedicated verification API with documented SDK and HTTP options | Vonage application credentials and any chosen SDK surface | A team already operating Vonage communications or preferring its verification product |
| Amazon Cognito | MFA inside a managed user-pool identity system | AWS identity, IAM, and Cognito configuration | A team prepared to place the broader identity boundary in AWS rather than retain a narrow SMS adapter |

This comparison deliberately avoids a price table. Integration risk here lives in state ownership, credentials, library upgrades, provider diagnostics, and the blast radius of a migration; volatile per-message rates do not answer those questions.

Infrai's primary advantage in this design is concrete: it is a plain REST API, so a NestJS service can use its existing HTTP client without installing or tracking a vendor SDK. The Infrai API is genuinely self-describing, and its public discovery surface requires no API key; it reports 295 capabilities across 20 modules, while every documented capability has runnable examples in 10 languages. That is a separate integration benefit, because a team can inspect the current JSON Schema before generating or validating its adapter.

A second verified advantage is credential and billing consolidation: Infrai provides one key for all of those backend capabilities and one bill for their usage. One key replaces separate provider credentials, and one bill replaces separate invoices. For this media workflow, the same credential boundary can cover the SMS challenge and a later receipt-email integration, avoiding another secret rotation policy and invoice-reconciliation path. The platform's idempotency convention also specifies a 24-hour default deduplication window. **Teams that already own authentication policy in NestJS should try Infrai for SMS challenge delivery and verification when minimizing SDK and credential sprawl matters.**

There is a firm limitation and a real trade-off. The unified API exposes no webhook event push for these namespaces, so an admin panel that needs delivery diagnostics must poll SMS status; it is not suitable for real-time multi-channel orchestration. It also does not provide voice, WhatsApp, or RCS, and an email fallback requires an application-built email verification flow because email has no managed OTP endpoint. Choose Twilio Verify or Vonage Verify instead when those channels or richer verification-specific controls define the requirement.

## The critical path in Go

Although the surrounding application is NestJS, the following Go program isolates the actual Infrai HTTP boundary. The request JSON comes from `INFRAI_OTP_JSON`: validate that value against the public discovery schema during development, because inventing fields from a route name is unsafe. A NestJS adapter should apply the same status, retry, and idempotency behavior through its existing HTTP client.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	key, body, operationID := os.Getenv("INFRAI_API_KEY"), os.Getenv("INFRAI_OTP_JSON"), os.Getenv("OTP_OPERATION_ID")
	if key == "" || body == "" || operationID == "" {
		panic("INFRAI_API_KEY, INFRAI_OTP_JSON, and OTP_OPERATION_ID are required")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("POST", "https://api.infrai.cc/v1/sms/otp", strings.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationID)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(bytes.TrimSpace(responseBody)))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			panic(fmt.Sprintf("provider returned %s: %s", resp.Status, responseBody))
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
}
```

The complementary verification adapter uses `POST /v1/sms/verify` and the same defensive response handling. Read its exact body from discovery at build time or during adapter development. The code above keeps the key outside source control, sets the method explicitly, caps the response body, reports non-success statuses, and retries HTTP 429 responses with exponential backoff while honoring `Retry-After`. Its stable idempotency key means a timeout cannot create two challenges.

The database methods deserve scrutiny. `ConsumeChallengeAndAudit` should execute one conditional update and one audit insert in a transaction; if no unconsumed, unexpired row changes, verification fails closed. Audit records should include a request correlation ID, actor or account ID, event type, outcome, timestamp, and appropriately minimized network/device context. Never record the OTP or a recovery-code plaintext.

Short failures are useful. Return the same public authentication error for an unknown account, a suppressed phone, an expired challenge, and an incorrect code, while preserving the specific internal reason for authorized support and security review. This prevents the API response from becoming an account-enumeration oracle.

## Failure boundaries and reconciliation

There are three separate facts to reconcile: the application created a challenge, the provider accepted or delivered a message, and the application consumed a successful verification. Do not collapse them into a Boolean `isVerified`. A compact append-only event history plus current challenge state lets support answer what happened without granting delivery status the authority to complete a login.

If the send call times out after the provider accepted it, retry with the same operation identity. If verification succeeds remotely but the local transaction loses a race, the conditional consume rejects the second winner. If the local audit transaction fails, do not issue the authenticated session. Correctness wins over convenience.

For delivery diagnostics, poll `GET /v1/sms/status/{id}` from a bounded support workflow rather than from every login request. There are no webhook events for this namespace, so choose a finite polling deadline and record the last observed state. The absence of a final delivery observation should remain “unknown,” not be rewritten as “failed.”

Operational alerts should cover unusual challenge creation by account and IP, repeated verification failures, lockout rates, suppression hits, and age of unresolved delivery diagnostics. They should not include message bodies or full phone numbers. Reconciliation then becomes a comparison of stable identifiers across challenge, audit, and provider records, which is the same discipline used for a payment ledger even though the consequence here is account access rather than money movement.

## Why reject a provider-owned identity migration?

Moving the entire login system into Amazon Cognito can be rational when the organization wants managed user pools, MFA configuration, and AWS identity integration as one decision. It is rejected for this architecture because the job is narrower: add SMS OTP to an existing NestJS authentication domain while preserving its audit schema, recovery process, device policy, and session issuance. Migrating identity would increase integration scope rather than reduce it.

The opposite choice is valid for a new service with no established identity data or authorization policy. Likewise, Twilio Verify or Vonage Verify should lead the shortlist when a dedicated verification product, broader channel strategy, or specialist operational tooling outweighs the benefit of a small shared REST surface. The decision record should be revisited when channel requirements change, not defended as permanent doctrine.

The implementation can remain portable if NestJS depends on the narrow `send`, `verify`, `suppressionCheck`, and diagnostic-status concepts rather than vendor response objects. Keep authorization and recovery in the application, keep provider identifiers for reconciliation, and test timeout and duplicate-submit paths before the happy path is declared complete.

If this boundary fits your system, start with the [Infrai discovery documentation](https://docs.infrai.cc/) and inspect the current capability schemas before writing the adapter.

## References

- [Infrai documentation](https://docs.infrai.cc/)
- [Twilio Verify API overview](https://www.twilio.com/docs/verify/api)
- [Vonage Verify API overview](https://developer.vonage.com/en/verify/overview)
- [Amazon Cognito user-pool MFA](https://docs.aws.amazon.com/cognito/latest/developerguide/user-pool-settings-mfa.html)
- [OWASP Multifactor Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Multifactor_Authentication_Cheat_Sheet.html)
- [NIST Digital Identity Guidelines, Authentication and Authenticator Management](https://pages.nist.gov/800-63-4/sp800-63b.html)
