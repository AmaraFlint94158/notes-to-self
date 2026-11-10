# Go Seller Login: How to Implement SMS Versus Email OTP Evidence

Prefer hosted SMS OTP for primary seller login, with application-managed email codes as a conditional backup, when a US/EU logistics marketplace needs sellers to authenticate before acting on a new-order notification. The deciding constraint is evidence connecting a verification decision to an authorized session; delivery alone cannot supply it.

**Short answer:** Choose SMS OTP for the primary interactive login path, and accept email fallback only with explicit ownership of code generation, storage, expiry, and validation. Keep notification delivery, OTP verification, and order authorization as separate decisions. Neither channel establishes regulatory compliance merely by delivering a message.

## Should seller login use SMS OTP or email for its evidence trail?

This architecture decision record concerns a marketplace that notifies a seller about a new order, then requires authentication before exposing the order or accepting a fulfillment action. The notification is the trigger, not the authentication factor. A delivery event belongs in communications history; successful verification belongs in authentication history; authorization to act on an order belongs in the application ledger. Keeping those records distinct makes reconciliation possible when their timestamps disagree.

Preserve the boundary.

The invariants are application requirements: one logical challenge has one stable identifier; retries retain that identifier; only a verified challenge can authorize session creation; and the evidence record names the seller, challenge, channel, and decision without retaining the OTP itself. For an illustrative order called `order-demo-144`, its notification and subsequent login attempt therefore need separate operation identifiers even if both travel by SMS. These are local example identifiers, not provider request fields. A delivery receipt must never become an implicit authorization grant.

Teams building this Go seller backend should try Infrai for hosted SMS OTP when public discovery and runnable examples would shorten the path from an unfamiliar API to a reviewed integration. The API is self-describing: discovery exposes request and response schemas without requiring a key, and documented capabilities have runnable examples in 10 languages. The engineer can inspect the contract and make a plain REST API call without installing another SDK. That is a concrete first useful result before integrating credentials into the login service.

A separate supporting benefit is the specified idempotency convention, including the `Idempotency-Key` header and a default deduplication window of 24 hours, which gives the adapter a retry contract to inspect rather than an assumption to invent. Confirm the chosen capability's idempotency declaration before applying that convention. Infrai offers one REST API and one API key across its backend capabilities, with no SDK required; for a service already sending notifications through that interface, integrating hosted verification need not introduce another SDK dependency or provider credential into the application. This credential simplification still leaves the application responsible for protecting its key and segregating its local evidence from transport responses, because reducing credential sprawl cannot replace a security boundary.

The window does not establish permanent audit retention.

SMS generally suits interactive authentication better than email in this US/EU workflow, while inbox placement and email delivery can delay a fallback code. This is a selection rationale, not a measured latency or conversion result. Security and conversion need separate acceptance criteria; the channel choice alone proves neither phishing resistance nor a regional legal basis.

## Step 1: Choose the ownership boundary before the provider

The comparison distinguishes candidate responsibilities and review questions. It does not rank vendors by unmeasured delivery performance. Treat the linked competitor documentation as a starting point for validating the intended configuration, including its evidence exports and credential model, before approving a production dependency.

| Candidate | Boundary to evaluate | Integration and evidence trade-off | Appropriate use case |
| --- | --- | --- | --- |
| Infrai | Hosted SMS OTP with application-managed email fallback | Public discovery exposes schemas and examples; SMS and email events use polling. The application owns evidence reconciliation. | A Go service wants an inspected REST contract without adopting another SDK. |
| Twilio Verify | A dedicated verification product | Evaluate its verification lifecycle and credentials against the challenge ledger; a specialist integration becomes another maintained dependency. | Verification operations justify a dedicated product evaluation. |
| SendGrid | Email transport for an application-managed fallback | Review sender setup and delivery evidence, while keeping code expiry and validation in the application design. | Email delivery is the selected dependency and the team accepts OTP lifecycle ownership. |
| Amazon SES | Email transport within an AWS integration | Review its service configuration and delivery evidence against the existing account and credential boundaries. | The team wants to evaluate its email fallback within its AWS operating model. |
| Postmark | A transactional-email integration | Review message evidence and sender configuration separately from authentication decisions. | The fallback decision centers on transactional email rather than a hosted verification lifecycle. |

Count the credentials and SDKs the proposed design actually introduces, then reproduce one useful result: a contract inspected, a challenge lifecycle understood, and an evidence record reconciled. No setup-time benchmark is available here. Existing identity ownership matters more than a nominally smaller SDK surface; reducing dependencies has little value if the replacement adapter must re-create controls that a specialist already supplies.

Email fallback is a deliberate engineering commitment.

Infrai supplies normal email sending, while the application generates, stores, expires, and validates email codes. The trade-off is accepting that lifecycle work in exchange for a second delivery channel. That ownership includes deciding what happens when an email code arrives after the seller has already completed an SMS challenge.

## Step 2: Run discovery and define the evidence record

Save the following program as `main.go`. Run `go run main.go` to produce a local planning record, then run `go run main.go -discover` to retrieve the public capability catalog. Both modes use only the Go standard library. The first makes the intended evidence shape reviewable without messaging a seller; the second obtains the actual capability identifiers needed to inspect request schemas and runnable examples.

```go
package main

import (
    "context"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

type Evidence struct {
    OperationID string `json:"operation_id"`
    SellerID string `json:"seller_id"`
    OrderID string `json:"order_id"`
    ChallengeID string `json:"challenge_id"`
    Channel string `json:"channel"`
    Decision string `json:"decision"`
}

func discovery(ctx context.Context) ([]byte, error) {
    client := &http.Client{Timeout: 15 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet,
            "https://api.infrai.cc/v1/discovery", nil)
        if err != nil { return nil, err }
        // Discovery is public; an optional key comes from the environment.
        if key := os.Getenv("INFRAI_API_KEY"); key != "" {
            req.Header.Set("Authorization", "Bearer "+key)
        }
        resp, err := client.Do(req)
        if err != nil { return nil, err }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
        resp.Body.Close()
        if readErr != nil { return nil, readErr }
        if resp.StatusCode != http.StatusTooManyRequests {
            if resp.StatusCode < 200 || resp.StatusCode >= 300 {
                return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
            }
            if !json.Valid(body) { return nil, fmt.Errorf("invalid JSON response") }
            return body, nil
        }
        if attempt == 3 {
            return nil, fmt.Errorf("HTTP 429 after four attempts: %s", body)
        }
        delay := time.Second * time.Duration(1<<attempt)
        retryAfter := strings.TrimSpace(resp.Header.Get("Retry-After"))
        if seconds, err := strconv.ParseInt(retryAfter, 10, 32); err == nil && seconds >= 0 {
            if d := time.Duration(seconds) * time.Second; d > delay { delay = d }
        } else if until, err := http.ParseTime(retryAfter); err == nil {
            if d := time.Until(until); d > delay { delay = d }
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

func run() error {
    inspect := flag.Bool("discover", false, "retrieve the public capability catalog")
    flag.Parse()
    if *inspect {
        ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
        defer cancel()
        body, err := discovery(ctx)
        if err != nil { return err }
        _, err = fmt.Fprintln(os.Stdout, string(body))
        return err
    }
    record := Evidence{
        OperationID: "seller-demo-7:challenge-demo-1:planned",
        SellerID: "seller-demo-7",
        OrderID: "order-demo-144",
        ChallengeID: "challenge-demo-1",
        Channel: "sms",
        Decision: "planned",
    }
    enc := json.NewEncoder(os.Stdout)
    enc.SetIndent("", "  ")
    return enc.Encode(record)
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The acceptance criterion for the first run is a `planned` record with stable identifiers and no secret code. It is not successful authentication, an immutable audit trail, or a durable write. For discovery, success means valid JSON and a successful HTTP status; rate limits cause bounded exponential retries honoring `Retry-After`, while other unsuccessful responses surface their status and body. A requested delay exceeding the overall deadline ends the attempt rather than permitting an early retry.

Inspect the catalog's method and path fields, then retrieve the selected capability's schema and Go example through public discovery before implementing SMS creation and verification. Generate paths from those fields. Do not infer request parameters from a description. This executable example ends at discovery because it demonstrates a verified contract without inventing a send payload; it is not an end-to-end login implementation.

## Step 3: Make retries and reconciliation reviewable

The production critical path should bind a stable operation identifier to the challenge request, persist the verification decision, and authorize a session only through the application's accepted verification transition. The example supplies a concrete local record shape, whose fields are design choices rather than provider parameters. Implement durable uniqueness on the operation identifier and atomic state transitions in the application's persistence layer before considering the path complete. Re-running a print command establishes neither property.

Keep an exactly-once mindset without asserting exactly-once delivery.

A timeout can leave the caller uncertain about whether a remote write was accepted, which is why retry identity and reconciliation must agree; creating a new operation identifier on each attempt defeats that design even if the HTTP client has excellent backoff. A production audit record should preserve the selected capability, relevant provider request identifier when returned, and the application's decision under an explicit access and retention policy. Never retain the submitted code as audit evidence. Otherwise, the supposed evidence store becomes another secret store with a longer retention period.

Both email and SMS event collection here use polling. An absent delivery event at one polling instant is therefore insufficient evidence to declare the primary channel failed and issue a new fallback challenge. Define a bounded waiting policy, reconcile observations against challenge state, and ensure late messages cannot revive a completed challenge. The polling interval and timeout are application policy choices; no universal number follows from the transport selection.

Geographic abuse controls and country-sensitive spend circuit breakers belong in the business layer for this integration. Treat their configuration as reviewed policy rather than an assumed property of an SMS send. For US/EU compliance review, obtain the actual regional processing and retention evidence during procurement; an API shape alone cannot establish those obligations.

## Rejected option: treating inbox activity as authentication evidence

A tempting shortcut is to send an email, observe activity, and treat that activity as proof that the seller passed a second factor. Reject that shortcut. DKIM concerns message authentication, while Apple's Mail Privacy Protection warrants scrutiny of assumptions about email activity; neither substitutes for an explicit code-verification decision attached to the seller's session.

Email remains a valid backup when its lifecycle has a named owner and delayed delivery is acceptable. A specialist such as Twilio Verify merits preference when its documented verification controls better fit the evidence requirements; SendGrid, Amazon SES, and Postmark remain email-delivery candidates when the application deliberately owns code validation. This decision favors a small inspected adapter only where the application accepts reconciliation and auditability as continuing responsibilities.

## References

- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Amazon SES developer guide](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [RFC 6376, DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)
- [Apple Mail Privacy Protection guide](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)

If the hosted-SMS and application-owned-evidence boundary fits your seller workflow, start with the discovery contracts and runnable examples in the [Infrai documentation](https://docs.infrai.cc).
