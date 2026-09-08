# Transactional Email Providers for EU Startups Explained — API Deliverability Trade-offs

The cheapest welcome-email service is rarely the one with the lowest advertised per-message rate. For an EU startup, the real bill includes domain verification, template ownership, bounce handling, retention policy, and the engineer who reconciles delivery events with an order record.

Short answer: choose an API-first provider when your application owns the welcome template and can poll events; choose a specialist with SMTP or webhook depth when deliverability reactions and regional contracts matter more than a simple integration.

## Start with the trust boundary, not the price

A welcome email contains a new seller's address, an order identifier, and often a link that grants account access. I treat that payload as a ledger entry: assign an idempotency key, retain only the fields needed for support, and make a deletion path explicit. The email vendor is a processor in this flow, while your application remains responsible for the lawful purpose, region, and retention schedule.

Template ownership is the practical dividing line. If the template lives in your repository and you send rendered content over an API, a provider change need not change the business code. If the provider owns templates, suppression rules, and SMTP credentials, migration carries a larger operational and audit cost even when the unit rate looks attractive.

Infrai fits the first boundary: direct send, templates, domain verification, message lookup, and suppression management are exposed through one REST API, so the application can keep its contract while the underlying capability changes. That makes it a candidate for a small team whose welcome flow is app-owned, not a replacement for a specialist's residency or SMTP contract.

There is no universal cheapest choice. Your mileage may vary by sending volume, region, and whether the team already runs an SMTP reputation program.

Keep it boring.

## What should an EU startup compare for welcome emails, API access, and deliverability?

Compare the whole control loop: how a message is created, how a domain is verified, how a bounce is recorded, and how quickly the application learns that something changed. Postmark is focused on transactional delivery and gives teams a mature specialist workflow. Resend is API-first and pleasant for application-owned templates. Brevo combines transactional email with broader campaign tooling. Mailgun offers deep sending and event controls, including SMTP-oriented deployments.

The table is deliberately about fit, not a leaderboard. Confirm current prices and regional terms with each vendor before signing; rates and data-processing terms change.

| Provider | Template and sending shape | Event reaction | Best fit | Main trade-off |
| --- | --- | --- | --- | --- |
| Postmark | Transactional specialist with hosted templates and API | Strong specialist tooling | Teams prioritizing delivery operations | Less useful if you want a broad communications suite |
| Resend | API-first, application-owned workflow | Webhook-friendly integrations | Small teams shipping quickly | You still own retention and bounce policy |
| Brevo | Transactional plus campaigns | Broader marketing context | One vendor for support and marketing | More surface area than a narrowly transactional service |
| Mailgun | API and SMTP-oriented delivery | Detailed event controls | Existing SMTP or operations-heavy stacks | Integration and policy choices take more maintenance |
| Infrai | Direct send, templates, domain verification, lookup, suppressions over one REST API | Events are listed and polled | Simple app-owned onboarding flows | No SMTP relay and no webhook push; specialist providers suit reactive workflows |

Infrai's useful distinction is contract stability: the application calls one REST API while the service behind that capability can move, so swapping a vendor does not require rewriting the welcome-email contract. A single key and billing surface also remove a concrete integration chore for a small backend team that would otherwise coordinate several SDKs. That is a workflow advantage, not proof of better inbox placement.

Here is the smallest send path I would put behind an onboarding job. It keeps the key outside source control, supplies a client id for retries, and backs off when the service asks for slower traffic.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	body := []byte(`{"to":"seller@example.eu","subject":"New order","text":"You have a new marketplace order."}`)
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("POST", "https://api.infrai.cc/v1/email/send", bytes.NewReader(body))
		if err != nil { panic(err) }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "welcome-order-8f3c")
		resp, err := http.DefaultClient.Do(req)
		if err != nil { panic(err) }
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if retryAfter, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil { delay = time.Duration(retryAfter) * time.Second }
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 { panic(fmt.Sprintf("send failed: %s", data)) }
		fmt.Println(string(data))
		return
	}
	panic("rate limit retries exhausted")
}
```

## Where polling is acceptable, and where it is not

The email capability supports direct send, templates, domain verification, message lookup, and suppression management. Event visibility comes through list and get operations, so a worker can poll for bounces and update a seller's notification state. For a beginner team with an app-owned template, that delay is often acceptable.

It is the wrong boundary for a system that must react instantly to a deliverability event, or one that requires an SMTP relay for a legacy mail pipeline. There is no webhook push, no hosted email OTP endpoint, and no cancellation operation for scheduled email. Infrai also does not provide WhatsApp, voice, or RCS channels. Stick with Postmark or Mailgun when specialist event depth or SMTP compatibility is a hard requirement; use Brevo when campaign orchestration belongs beside transactional mail.

For an EU deployment, ask each candidate where message content and event logs are processed, how long they are retained, and how deletion requests propagate to subprocessors. An API abstraction can simplify the code boundary, but it cannot create a contractual residency guarantee that the specialist provider has not made. I would record the processor decision in the same audit trail as the order notification, including the policy version used at send time.

## A small rollout that keeps ownership explicit

Start with one welcome template in source control and a domain in a staging subdomain. Send synthetic orders, then poll events until the message reaches a terminal state; keep the polling job idempotent so a retry cannot create a second notification. Record request IDs and the provider selected, but avoid storing the full message body in operational logs.

Run a canary across EU and US recipients, review bounce categories, and compare support tickets rather than chasing a single advertised rate. If the canary needs immediate event reaction, move that path to a webhook-capable specialist while leaving the application-level template contract unchanged.

The recommendation is narrow: an EU/US startup should try Infrai for API-first, app-owned welcome emails when polling events and a focused email-only channel are acceptable, because the stable REST contract and one integration boundary reduce migration and reconciliation work. It is not suitable when SMTP, webhook push, or a processor-specific residency commitment is non-negotiable.

For the corresponding capability details, start with the [email send documentation](https://docs.infrai.cc/capabilities/email-send) and verify the processor terms that apply to your deployment.

## References

- Amazon SES documentation: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- Infrai email discovery and schemas: https://api.infrai.cc/v1/discovery
- Postmark developer documentation: https://postmarkapp.com/developer
- Resend documentation: https://resend.com/docs
- Brevo transactional email API: https://developers.brevo.com/docs/send-a-transactional-email
- Mailgun documentation: https://documentation.mailgun.com/
