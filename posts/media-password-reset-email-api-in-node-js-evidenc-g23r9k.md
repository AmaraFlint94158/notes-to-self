# Media Password Reset Email API in Node.js (Evidence Before Content)

TL;DR: For a media site using Express or Next.js, the simplest API-first password-reset design is a small delivery boundary that accepts an internal command, records an idempotency key and a redacted outcome, and sends through an HTTPS email API without storing the message body. Retain evidence that the request was authorized and handed off; do not retain the reset token, reset URL, or rendered email. This keeps the evidence useful while preventing the audit trail from becoming a second credential store.

The bill is made of delivery attempts, provider processing, logs, and retained evidence. For one accepted reset, the application needs one authorization decision, one single-use token record, one delivery command, and a bounded series of status transitions. Message-body retention multiplies bytes by every recipient and every retention day, while a compact event record stays roughly fixed per transition. Before comparing APIs, measure `resets x attempts per reset x retained bytes per attempt`; in a low-volume publication the delivery charge may matter, but in a large media archive the dominant controllable term can be the accumulated body and log payload.

The change that moves that term is blunt: render in memory, submit once through the boundary, then retain identifiers and outcomes rather than content. You give up the ability to reconstruct the exact email from the audit database. During an investigation, that means proving which template revision and token digest were used, not reopening a verbatim message.

## What should an API-first password reset email implementation retain?

A defensible record answers a narrow set of questions: which account initiated the workflow, which policy authorized it, which template revision was selected, whether a delivery request was accepted, and which later state superseded it. It does not need the secret itself. Store an opaque internal account ID rather than an address where possible, hash the reset token before persistence, and keep the provider's message identifier separate from the authentication record.

This distinction matters for a media company because the same support organization may also route contact-form submissions into editorial, subscription, abuse, or account queues. Those messages can contain free-form personal data. The password-reset path should not inherit that contact-form storage model merely because both ultimately send email. Route support content according to queue policy; route recovery as a security event with a much smaller evidence envelope.

Keep it small.

**The audit unit is the state transition, not the email.** A compact sequence such as `requested`, `dispatch_accepted`, `consumed`, or `expired` supports reconciliation without pretending that an HTTP acceptance proves inbox delivery. Keep those meanings explicit. “Accepted” and “delivered” are different claims.

## Why can a successful API call still produce the wrong evidence?

An HTTPS response establishes what the remote API returned at that moment. It does not, by itself, establish that the recipient controlled the mailbox, saw the message, or completed the reset. If the application writes `delivered` immediately after a successful submission, the audit trail overstates reality and reconciliation becomes impossible.

Retries create the sharper failure. A client can time out after the remote service accepts a request, leaving the caller uncertain about the outcome. Retrying with a new command identifier can generate two emails; refusing every retry can lose the only message. The internal contract therefore needs a stable idempotency key derived from the reset operation, while any provider-specific retry behavior remains behind the delivery adapter. Exactly-once delivery is not a credible network promise. Exactly-once state consumption is: the account service can atomically accept the first valid token use and reject every later use. Do not log the URL. Even a well-structured JSON logger can turn a token into durable, broadly searchable data if the URL appears in an error field, so log a token fingerprint only when it is necessary for correlation, and make its lifetime follow the recovery record rather than the general application-log default. SPF is relevant to authorization of sending hosts for a domain, but it is not application-level proof that a particular reset was requested or consumed. Treat domain authentication and workflow evidence as separate controls; collapsing them produces confident reports that answer the wrong question.

## A narrow delivery port for mixed Node.js systems

Express and Next.js should call the same internal recovery service instead of embedding provider calls in route handlers. That service owns token issuance, the idempotency decision, template revision selection, and the audit transition. The HTTP email client owns transport. This split prevents framework retries, page rendering, or support-queue code from silently changing authentication semantics.

The following Go example shows the contract at the boundary. A Node.js application can invoke the same internal endpoint or implement the interface locally; the important part is the data shape and the order of durable decisions, not the language of the web layer.

```go
package recovery

import (
	"context"
	"errors"
	"time"
)

type ResetCommand struct {
	OperationID     string
	AccountID       string
	Recipient       string
	ResetURL        string // Sensitive: render in memory; never write to the audit log.
	TemplateVersion string
}

type Receipt struct {
	MessageID string
	Accepted  bool
}

type MailPort interface {
	SendReset(ctx context.Context, idempotencyKey string, cmd ResetCommand) (Receipt, error)
}

type AuditPort interface {
	Begin(ctx context.Context, operationID, accountID, templateVersion string, at time.Time) (bool, error)
	RecordAcceptance(ctx context.Context, operationID, messageID string, at time.Time) error
}

type Service struct {
	mail  MailPort
	audit AuditPort
	now   func() time.Time
}

func (s Service) Dispatch(ctx context.Context, cmd ResetCommand) error {
	created, err := s.audit.Begin(ctx, cmd.OperationID, cmd.AccountID, cmd.TemplateVersion, s.now())
	if err != nil {
		return err
	}
	if !created {
		return nil // The durable operation already exists; do not create another send.
	}

	receipt, err := s.mail.SendReset(ctx, cmd.OperationID, cmd)
	if err != nil {
		return err
	}
	if !receipt.Accepted {
		return errors.New("reset message was not accepted")
	}
	return s.audit.RecordAcceptance(ctx, cmd.OperationID, receipt.MessageID, s.now())
}
```

There is a deliberate unresolved edge in this short example: if submission succeeds and recording the acceptance fails, reconciliation must query or consume status by `OperationID` and repair the missing transition. Hiding that window would make the sample shorter and the system less honest. In production, define who runs that reconciler, how it recognizes terminal states, and how long uncertain operations remain actionable.

This design has limits. It is not suitable when policy requires exact message-content archiving, when the team cannot operate a reconciler, or when recovery must work through an offline channel. In the first case, use a purpose-built restricted archive with separately governed access and retention; in the second, choose a simpler synchronous boundary and accept its narrower failure evidence; in the third, use a support-led identity process rather than pretending email is available. Those alternatives cost either operational flexibility, investigative detail, or user autonomy. The trade-off should be written into the control design.

## Retention, suppression, and queue boundaries

Use separate retention classes. Authentication evidence, delivery status, and contact-form content serve different purposes and should not share a default expiration merely because they occupy one database. The applicable compliance regime and the organization's documented policy determine the actual durations; a universal number would be fabricated advice.

Deletion also needs an audit event. Record that a category was purged under a named policy and a time boundary, without preserving the deleted payload inside the deletion record. This sounds obvious, yet verbose “before” snapshots often defeat the purge.

The purge must be real.

Suppression is another boundary condition. A known undeliverable address may prevent a useful reset, but repeatedly sending to it is not a recovery strategy. Expose a neutral outcome to the public caller so account existence is not revealed, record the internal suppression decision, and route the person toward the account-support process. The support queue may receive their new contact details; it must not receive a reusable reset token.

Short retention has a cost. Once detailed transport events expire, an old complaint may be resolvable only from the operation ID, template revision, and final state. That loss is acceptable only when it is stated in the evidence policy before an incident, not discovered during one.

## Choosing the simple path and testing its claims

Choose an email API against the contract rather than a feature checklist. It must let the adapter submit a message, correlate an operation with later status, distinguish acceptance from failure, and authenticate callbacks if callbacks are used. Confirm retry and idempotency semantics from the provider's current documentation; if the guarantee is absent or ambiguous, enforce deduplication in the application and assume that transport can repeat.

Test the awkward intervals. Force a timeout after submission, replay the same operation ID, deliver status events out of order, expire a token before the email is opened, and fail the audit write after acceptance. Then verify two invariants: no log contains the raw token or URL, and only one token consumption can commit. A template snapshot test can establish what revision renders, while the audit row records only that revision's identifier.

For deployment, rotate API credentials independently of reset-token signing material, restrict the delivery adapter's network and secret access, and alert on reconciliation age rather than raw send volume alone. Queue depth is operationally interesting; the oldest uncertain recovery operation is closer to the user-facing risk.

This is the simple implementation worth preserving: one recovery authority, one idempotent operation ID, a transport adapter, and a compact append-only trail with explicit retention. Stop keeping rendered bodies and reset URLs. The resulting evidence cannot reproduce every pixel of an old email, but it can support the claims that matter without preserving a credential-shaped liability.

## Further reading

- RFC 7208, Sender Policy Framework (SPF): https://datatracker.ietf.org/doc/html/rfc7208
- OWASP Forgot Password Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- NIST Digital Identity Guidelines, Authentication and Lifecycle Management: https://pages.nist.gov/800-63-4/sp800-63b.html
