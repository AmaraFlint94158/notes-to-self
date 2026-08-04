# Choosing a Simple Node.js Queue for Safe Cleanup, Retry, and Dead Letters

## TL;DR

For scheduled data cleanup in Node.js, the best simple setup is a durable background job queue with an enqueue-only schedule, bounded retries, and a dead letter queue that requires human review. Keep deletion and its audit record in one database transaction, make every delivery idempotent, and choose a more elaborate scheduler only when the workflow crosses services or must meet a hard compliance deadline.

The queue may deliver a job more than once. Design for that fact.

My architecture decision is deliberately narrow: one schedule creates a small batch job; one worker claims it; the handler records a stable idempotency key, deletes eligible rows, and writes an audit event atomically; transient failures retry with jitter; exhausted or deterministic failures go to a dead letter queue. This arrangement is easy to explain during reconciliation and small enough that a team can operate it without a separate orchestration practice.

It is not suitable when cleanup has several long waits, compensating actions, or acknowledgements from independent systems. In that case, use a durable workflow engine or explicit state machine, because a queue message is a poor substitute for visible workflow state. Nor would I use a queue for a harmless, single-statement maintenance query whose occasional delay has no business consequence; a plain schedule can be the clearer choice. The governing distinction is not job volume. It is whether the organization must prove what was selected, what was removed, which attempt committed, and what remains unresolved.

## How should a Node.js background job queue schedule cleanup retries and a dead letter queue?

Start by separating time, delivery, and deletion. The scheduler knows when cleanup should begin, but it doesn't delete anything; it inserts one durable message containing a run ID, a cutoff timestamp, and a batch limit. The queue controls leases and redelivery, but it doesn't decide whether an error is transient. The Node.js worker owns classification and calls a transactional cleanup boundary. That division gives each component one decision and leaves an audit trail that can be reconciled without reconstructing intent from logs.

Retries are delivery.

Use a finite attempt budget and exponential backoff with jitter for conditions that can plausibly clear, such as a short lock conflict or a temporary dependency timeout. Send malformed payloads, violated business invariants, and exhausted attempts to the dead letter queue immediately or after the configured limit. Don't consume that queue automatically: attach the run ID, error class, attempt count, first-seen time, and last-seen time, then alert on both its depth and the age of its oldest item. Redrive should create a new auditable decision linked to the original job, not erase the failure history.

The exactly-once mindset belongs at the transaction boundary, since a queue cannot know whether a worker committed just before its lease expired. Give each logical batch a stable idempotency key, insert that key into a table with a unique constraint, and place the deletion plus audit insert in the same transaction. A repeated delivery then observes the completed key and returns success without deleting again. If policy permits deleting data but requires retaining evidence, keep audit records free of the sensitive payload itself; record identifiers, policy version, counts, and timestamps rather than copying the data being erased. Retention and erasure obligations vary by jurisdiction and contract, so I won't invent a universal deadline: have counsel define the limit, then make the system expose whether every run met it.

## Which invariants and failure boundaries matter most?

I write the invariants before choosing any queue implementation because feature lists tend to hide the one boundary that will fail at 02:00. For a cleanup system, the useful invariants are: a cutoff never moves after enqueue; no row newer than that cutoff is eligible; one logical batch commits at most once; every committed deletion has an audit event; no failed attempt is mistaken for completed work; and every terminal failure becomes visible to an operator. Those are testable claims. “Reliable jobs” is not.

| Option | Useful boundary | Main limitation | Appropriate use |
| --- | --- | --- | --- |
| Database-backed queue | Enqueue can share a transaction with application state | Queue load competes with application database work | Cleanup targets the same database and throughput is moderate |
| Separate durable broker | Queue survives an application database outage | State change and enqueue need an outbox or reconciliation process | Workers and producers have independent failure domains |
| Plain scheduled runner | Few components and a direct execution path | Delay, overlap, and missed-run evidence require extra work | One idempotent statement with a soft completion window |
| Durable workflow state machine | Each step and wait is explicit | More operational and modeling overhead | Multi-service cleanup with acknowledgements or compensation |

My nastiest cost surprise came from ignoring the batch boundary. I estimated $45 for a month of queue and database activity, then received a $612 bill after a retention-policy migration selected 3.1 million records and emitted one message per row; retries multiplied writes, verbose payloads inflated storage, and completed messages remained retained for thirty days. Nothing exotic had failed — my cardinality assumption had. We changed the producer to enqueue bounded batches, stored only identifiers and cutoffs in messages, capped completed-message retention, and added a preflight estimate comparing candidate rows with the prior seven-day median. I'm not sure why I trusted the ordinary nightly volume during a policy migration, but the audit table made the cause embarrassingly clear.

Failure boundaries also determine observability. Track scheduled runs, enqueue lag, claim lag, execution duration, attempts, rows examined, rows deleted, idempotent replays, dead-letter depth, and oldest dead-letter age. Logs help diagnose an attempt; counters and durable run records prove whether the cleanup obligation finished. Your mileage may vary on alert thresholds, but “oldest unresolved item exceeds the policy window” is a better page than a generic worker-error rate.

## What does the critical cleanup path look like?

The following Go code shows the database boundary I want behind a Node.js worker. It is intentionally an interface-level example rather than a package tutorial: the worker supplies a stable job ID and immutable cutoff, while the transaction enforces one committed result. In a real schema I also constrain `job_id` as unique and restrict the audit principal to append-only writes.

```go
package cleanup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

func RunBatch(ctx context.Context, db *sql.DB, jobID string, cutoff time.Time, limit int) (int64, error) {
	if jobID == "" || limit < 1 || limit > 1000 {
		return 0, fmt.Errorf("invalid cleanup job")
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var inserted string
	err = tx.QueryRowContext(ctx, `
		INSERT INTO cleanup_runs (job_id, cutoff_at, status, started_at)
		VALUES ($1, $2, 'running', CURRENT_TIMESTAMP)
		ON CONFLICT (job_id) DO NOTHING
		RETURNING job_id`, jobID, cutoff).Scan(&inserted)
	if errors.Is(err, sql.ErrNoRows) {
		var status string
		if err := tx.QueryRowContext(ctx,
			`SELECT status FROM cleanup_runs WHERE job_id = $1`, jobID).Scan(&status); err != nil {
			return 0, err
		}
		if status == "completed" {
			return 0, tx.Commit()
		}
		return 0, fmt.Errorf("job already active: %s", jobID)
	}
	if err != nil {
		return 0, err
	}

	result, err := tx.ExecContext(ctx, `
		WITH candidates AS (
			SELECT id FROM retained_records
			WHERE expires_at < $1
			ORDER BY expires_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		), audited AS (
			INSERT INTO cleanup_audit (job_id, record_id, deleted_at)
			SELECT $3, id, CURRENT_TIMESTAMP FROM candidates
		)
		DELETE FROM retained_records
		WHERE id IN (SELECT id FROM candidates)`, cutoff, limit, jobID)
	if err != nil {
		return 0, err
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `
		UPDATE cleanup_runs
		SET status = 'completed', deleted_count = $2, finished_at = CURRENT_TIMESTAMP
		WHERE job_id = $1`, jobID, deleted); err != nil {
		return 0, err
	}
	return deleted, tx.Commit()
}
```

The unique run row is the idempotency gate; row locks prevent overlapping batches from selecting the same records; and audit, delete, and completion commit together. Test the ambiguous case — commit succeeds but the worker loses its lease before acknowledging — by forcing redelivery of the same `jobID`. The second call must perform zero new deletion and report success. Also test boundary timestamps, concurrent workers, rollback after audit insertion, poisoned payload routing, retry exhaustion, and redrive with a new job ID linked to the old one. This is the smallest test suite I would accept before allowing destructive work to run unattended.

If a job can call a URL supplied by a tenant or by stored data, treat that as an SSRF boundary — scheduled execution doesn't make the destination trustworthy. OWASP recommends allowlisting expected destinations where possible and validating both hostnames and resolved IP addresses; apply those controls before connecting, restrict outbound network access, and avoid following redirects into an unapproved address range. A cleanup worker often has unusually broad database and storage permissions, so its network surface deserves the same scrutiny as its delete statement.

## Why reject an inline cron job, and when is it still valid?

I reject an inline cron job as the default because scheduling and destructive execution then share one process lifetime. A delayed start, overlapping invocation, or process exit can leave ambiguous evidence: did the run never begin, finish partially, or commit and lose its final log line? Adding a durable run table can repair much of that ambiguity, but once the application also needs retries, leases, bounded concurrency, and a dead-letter review path, it has recreated a queue in pieces.

There is a valid use case. Stick with a plain scheduled runner when the operation is one bounded, idempotent database statement; a missed or delayed run does not violate a contractual or regulatory window; the next run safely catches up; and the run table records start, finish, cutoff, count, and error. Fewer components can improve reliability when the omitted semantics genuinely aren't needed. For repositories already using a hosted source-control runner, a scheduled workflow can be adequate for low-stakes housekeeping, but its documented behavior matters: scheduled events can be delayed during high load, high-load periods include the start of each hour, the shortest interval is five minutes, and scheduled workflows in public repositories are automatically disabled after 60 days without repository activity. Those constraints rule it out for a deadline whose breach needs immediate operational ownership.

The catch is sharp. Don't let a convenient timer become the system of record for compliance. If a cleanup must complete within a defined limit, persist each expected run independently of the scheduler, detect missing runs, and alert before the deadline rather than after it. If deletion spans multiple services — object storage, search indexes, analytics copies, and a closing acknowledgement — choose explicit durable workflow state. Each step needs its own idempotency key and audit event, and the final state must distinguish “requested,” “partially completed,” “blocked,” and “verified.” A dead letter queue remains useful for failed deliveries, but it cannot express the business state of a multi-step erasure.

My final decision rule is plain: use the least machinery that can prove completion after an ambiguous failure. For most Node.js scheduled cleanup, that is one durable queue, one small batch shape, one transactional idempotency boundary, bounded retry, and a dead letter queue owned by a named team. Anything simpler must show how it recovers missed work; anything larger must justify the new state it introduces.

## References

- OWASP, “Server Side Request Forgery Prevention Cheat Sheet”: https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html
- GitHub Docs, “Events that trigger workflows” (`schedule`): https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
