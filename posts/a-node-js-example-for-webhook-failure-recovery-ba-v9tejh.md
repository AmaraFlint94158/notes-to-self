# A Node.js Example for Webhook Failure Recovery: Backoff, DLQ, and Redrive

Short answer: put each failed webhook into a durable retry queue, calculate exponential backoff from a persisted attempt number, move exhausted work to a dead letter queue, and permit redrive only through an idempotent, audited operation.

The governing constraint is stronger than “try the HTTP request again.” A delivery worker can stop after sending but before recording success, a receiver can accept the event but lose its response, and an operator can replay a dead letter while an old lease is still alive. The sender therefore cannot prove exactly-once delivery across the network. It can, however, approach an exactly-once outcome by combining at-least-once attempts with stable event identities, receiver-side deduplication, state transitions that are committed atomically, and an audit trail that explains every attempt.

## What should a Node.js webhook retry queue record before exponential backoff and DLQ redrive?

Persist an immutable delivery identity and mutable execution state separately. The identity should bind the event, destination, and payload version; the execution record should carry the current state, attempt count, next eligible time, lease owner and expiry, last outcome classification, creation time, and a reference to the immutable payload. A payload hash is useful for detecting accidental mutation, while an idempotency key gives the receiver a stable deduplication key.

Do not let the queue message become the system of record. A queue can tell workers that an item may be ready, but the database record should decide whether it is actually eligible. This distinction matters during redelivery: two workers may observe the same notification, yet a conditional state transition from `scheduled` to `in_flight` allows only one valid lease holder to perform the attempt.

The state machine can remain compact:

| State | Allowed next states | Required audit evidence |
| --- | --- | --- |
| `scheduled` | `in_flight` | eligibility time and attempt number |
| `in_flight` | `delivered`, `scheduled`, `dead_lettered` | lease identity and classified outcome |
| `dead_lettered` | `scheduled` | actor, reason, and redrive batch identity |
| `delivered` | none | receiver acknowledgement metadata |

Keep transitions monotonic wherever possible. In particular, a late worker must not overwrite `delivered` with `scheduled`, and redrive must not erase the earlier failure history. Auditability is part of correctness here, not an observability accessory.

## Backoff is a scheduling policy, not a sleep call

Exponential backoff should produce a future eligibility timestamp and release the worker. Sleeping inside a worker ties capacity to waiting, complicates deployment, and leaves the schedule implicit in process memory. A persisted `next_attempt_at` makes the decision inspectable and lets a scheduler publish only eligible work.

A practical policy has four inputs: a base delay, an attempt number, a maximum delay, and jitter. Jitter prevents many deliveries that failed together from becoming eligible at exactly the same instant. The exact distribution is a policy choice; I'm not sure one distribution is universally superior, because the answer depends on fleet size, receiver capacity, and the precision of the scheduler. What matters is that the chosen formula is documented, bounded, and testable with an injected random source and clock.

Keep it explicit.

The following Go example models the calculation that a Node.js worker would implement around its queue client; the language binding changes, but the persisted invariants do not. It uses full jitter over the bounded exponential window and avoids shifting beyond the configured cap.

```go
package delivery

import (
	"math/rand"
	"time"
)

func NextAttempt(now time.Time, attempt uint, base, capDelay time.Duration, rng *rand.Rand) time.Time {
	window := base
	for i := uint(1); i < attempt && window < capDelay; i++ {
		if window > capDelay/2 {
			window = capDelay
			break
		}
		window *= 2
	}
	if window > capDelay {
		window = capDelay
	}
	delay := time.Duration(rng.Int63n(int64(window) + 1))
	return now.Add(delay)
}
```

The attempt counter must advance in the same transaction that records the failed outcome and next eligibility time. Otherwise, a process interruption between those writes can repeat the same backoff tier or skip one. This is the sort of small bookkeeping gap that later turns into an irreconcilable timeline — the queue says one thing, the database another, and neither can establish which policy actually ran.

## Classify outcomes before retrying

“Failed” is too coarse for a retry decision. Divide outcomes into terminal rejection, transient delivery failure, policy pause, and indeterminate completion. An invalid destination or a receiver response that definitively rejects the contract belongs on a terminal path; a time-limited transport failure can return to `scheduled`; a disabled tenant should remain paused without consuming attempts; and a connection lost after the request body was sent is indeterminate because the receiver may already have committed the event.

That last case is why the idempotency key cannot change between attempts. Signatures and idempotency solve different problems: an HMAC authenticates a message using a shared secret, as specified by RFC 2104, while an idempotency key lets application logic recognize the same delivery. Include a timestamp and key identifier in the signed envelope so the receiver can enforce its replay policy and rotate secrets, but do not claim that a valid signature alone prevents an authenticated message from being submitted twice.

The receiver should record the idempotency key and its committed result atomically. If it cannot do that, exactly-once effects remain an aspiration. For payment or ledger mutations, deduplication has to cover the business transaction itself; checking a short-lived cache before a separate ledger write leaves a crash window between the two operations.

Beware retries after ambiguous timeouts.

## A dead letter queue is evidence, not a trash bin

Move work to the DLQ after the configured attempt or age budget is exhausted, or immediately after a terminal classification. Preserve the payload reference, destination, attempt history, signature key identifier, outcome summaries, and state-transition log. Sensitive headers and payload fields require the same retention and access controls as the source event; an operational queue doesn't create an exemption from privacy, financial-record, or contractual retention obligations.

Redrive should create a new, reviewable execution episode linked to the original delivery. Require an operator or automation identity, a reason, a bounded selection predicate, and a unique batch identifier. Then perform a conditional transition that affects only records still in `dead_lettered`; rerunning the same batch must be harmless. A dry-run count and sampled reason distribution are useful before mutation, while rate limits and concurrency caps keep a repaired receiver from being overwhelmed by accumulated work.

The catch is that a DLQ is not suitable as indefinite archival storage. Retention limits can delete the very evidence needed for reconciliation, and unrestricted replay can reintroduce events after their business validity has expired. Keep an immutable audit record in storage designed for the applicable retention regime, and use the DLQ as a bounded operational surface. If a workflow requires human approval, legal holds, or multi-step compensation, stick with an explicit workflow engine or case-management process rather than stretching a basic retry queue into one.

## Comparison and rollout

There are three defensible implementation shapes. A database-backed scheduler offers strong transactional coupling between business state and delivery state, but polling and partition maintenance become your responsibility. A broker with delayed delivery can provide efficient dispatch and isolation, but atomicity across the broker and application database needs an outbox or equivalent reconciliation mechanism. A workflow engine makes timers and long-running histories first-class, although it adds operational concepts and may be excessive for a small, high-volume callback with a short lifetime. Sidekiq's documented retry and dead-set behavior is one concrete reference for how a mature job system exposes retries and exhausted work, not a universal contract for other queues.

Roll out in narrow stages: first dual-write the new audit record without changing dispatch, then enable idempotency-key acceptance at receivers, then move a small destination cohort to persisted scheduling, and only afterward enable bounded redrive. At every stage, reconcile counts across created, eligible, leased, delivered, rescheduled, and dead-lettered records; alert on impossible transitions and aging work rather than relying only on aggregate success rate. Deployment rollback should stop new leasing while preserving recorded eligibility times, so returning to the prior worker doesn't silently reset attempt history.

Test with a controllable clock and deterministic random source. Cover a process stop before send, a stop after send but before acknowledgement, lease expiry, duplicate queue notifications, a late acknowledgement, concurrent redrive, and secret rotation. The acceptance criterion is not merely that the eventual request arrives. It is that every externally visible effect has one stable identity, every retry decision can be reconstructed, and every terminal or replay action has an accountable actor.

## References

- https://www.rfc-editor.org/rfc/rfc2104
- https://github.com/sidekiq/sidekiq/wiki
