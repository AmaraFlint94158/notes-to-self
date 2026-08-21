# Per-Tenant LLM JSON Extraction Accounting in Node.js: Batch or Realtime?

Short answer: make the tenant and operation ID part of the extraction ledger before comparing models, count tokens with a versioned tokenizer policy, and choose batch or realtime from the caller's latency contract rather than from a headline price.

The accounting boundary comes first. A marketplace that turns listings, invoices, or review text into structured JSON can report an attractive average cost while one large tenant silently consumes the budget, or while a retry charges the same document twice. Neither outcome is acceptable when the output feeds a payment, settlement, or trust workflow.

This is a cost-control problem with a correctness obligation. Valid JSON is only the outer shell: a parser cannot tell you that a currency was changed, an amount lost its decimal precision, or a seller identifier was invented because the source was incomplete.

## Start with a tenant-level extraction ledger

Give every logical extraction a stable operation ID derived from the document identity, schema version, and extraction policy. Store the tenant ID separately, because a hash that prevents duplicate application is not a substitute for attribution. Each attempt should record the source hash, normalized-input hash, model identifier, tokenizer policy, execution mode, input and output token counts, estimated cost, validation result, and final disposition.

The ledger is append-only at the attempt level. A separate uniqueness rule allows one accepted result for an operation, so a redelivered queue message cannot apply two JSON results even if the transport delivers the message twice. This is an exactly-once mindset applied to the business effect, not a claim that the network provides exactly-once delivery.

The failure sequence is easy to miss in a happy-path test. A worker reads a listing document, submits it for extraction, receives a valid response, and writes the structured review while its acknowledgement to the queue is delayed. The queue redelivers the message; the second worker submits another request, receives another valid response, and increments the tenant's usage again. If the service keys only on a provider request identifier, those two requests can look unrelated even though they represent one business operation. A stable operation ID makes the relationship explicit, while an accepted-result uniqueness constraint makes the second write a no-op and an attempt ledger still records the transport retry. The usage report can then distinguish model work that was repeated from a second accepted marketplace action, which is important for both quota enforcement and financial reconciliation. This is why token counting by itself cannot be called cost control: it measures exposure, but idempotency determines whether repeated exposure can create repeated effects.

No shortcut.

For a marketplace, the useful report is not merely total spend. It is spend by tenant, document class, model, and outcome: accepted, sent to review, rejected by schema, rejected by a domain invariant, or still pending. Those categories make a rising bill diagnosable. They also make a tenant's chargeback or quota explainable months later.

Measure the ledger.

Keep the rejected payload and the validation evidence under the same operation ID. An auditor, or the engineer investigating a disputed review, should be able to reconstruct what the system received and why no downstream mutation occurred.

## How should a marketplace compare model cost for reliable JSON extraction?

First define the quality floor; then measure cost against it. A small labeled corpus should include ordinary text, long documents, missing fields, conflicting values, and source text containing instructions that must be treated as data. Run every candidate against the same schema and preserve the evaluation version. Record field-level correctness, schema acceptance, invariant failures, review rate, and token counts. Parseability alone is too weak a metric.

The model decision is therefore a constrained comparison: choose the least costly candidate that clears the declared quality and governance thresholds for a document class. Do not select a model because a public ranking looks favorable, and do not compare a short support message with a dense financial statement as though their token distributions were interchangeable.

| Integration option | Cost-control evidence | Best fit | Main limitation |
|---|---|---|---|
| Direct provider API | Provider usage plus the service's own token ledger | A team that needs a direct contract and a fixed model set | Provider-specific policies and migrations remain the caller's responsibility |
| Routing layer | Model, route, and token data captured at the application boundary | Experiments across several approved models | Routing policy and model availability require their own audit record |
| Self-hosted model | Local tokenizer, GPU usage, and queue metrics | Workloads with predictable volume and operational capacity | Infrastructure and model operations become part of the reliability budget |
| Hosted batch interface | Manifest, partition counts, and completion reconciliation | Backfills and work with no immediate response contract | Queue delay is incompatible with synchronous user workflows |

The table is a starting taxonomy, not a ranking. Each option still has to pass the same labeled corpus, domain invariants, retention rules, and per-tenant reporting test.

Token counting belongs before admission to execution. Normalize only what the policy permits, retain the original text, count the normalized input, and estimate output allowance from the schema. The official `tiktoken` project documents a BPE tokenizer implementation, but tokenizer alignment must still be checked against the selected model and message format; an estimate is a control signal, not an invoice.

Here is the shape of a language-neutral cost record. The calling service can implement it in Node.js or another runtime while keeping the accounting contract independent of its SDK:

```go
package ledger

import "time"

type ExtractionAttempt struct {
	OperationID    string
	TenantID       string
	DocumentClass  string
	SchemaVersion  string
	ModelID        string
	TokenizerID    string
	Mode           string
	InputTokens    int64
	OutputTokens   int64
	EstimatedUnits int64
	Outcome        string
	CreatedAt      time.Time
}

func Acceptable(a ExtractionAttempt) bool {
	return a.OperationID != "" &&
		a.TenantID != "" &&
		a.SchemaVersion != "" &&
		a.InputTokens >= 0 &&
		a.OutputTokens >= 0 &&
		a.Outcome == "accepted"
}
```

The code does not calculate a provider invoice. That omission is deliberate: the durable interface is the evidence needed to reconcile usage, while rates and model catalogs change. A production implementation should also persist the policy version that produced `EstimatedUnits`, and should reject an accepted result if monetary fields fail the domain invariants.

## Batch and realtime solve different failure windows

Realtime is appropriate when a person or synchronous API caller is waiting and latency is part of the contract. Batch fits nightly imports, seller-catalog backfills, and review queues without an immediate response requirement. Batch is not a correctness bypass; it is a different scheduling envelope.

Short rule: urgency selects realtime.

For batch, create a stable manifest before dispatch. Reconcile each partition with a simple equation: submitted documents equal accepted results plus rejected results plus pending documents. A partition is not complete while the counts do not balance. For realtime, bound concurrency, cap input size before dispatch, and make the timeout outcome distinct from a schema or domain-validation outcome.

Transport retries must retain the logical operation ID. Back off on rate limiting and honor the server's retry guidance, but do not create a new business operation merely because an HTTP attempt is repeated. Authentication failures, malformed responses, policy denials, and domain-invariant failures need separate classifications; otherwise, a retry loop can conceal a configuration error and inflate per-tenant spend.

The catch is that batch is unsuitable when a marketplace promise depends on an immediate result, while realtime is unsuitable for large backfills whose callers can tolerate a queue. Keep the mode decision in configuration by document class, and measure queue age, p95 latency, retry count, and cost per accepted extraction separately. I'm not sure one global threshold can serve every tenant; your mileage may vary with document mix and review policy.

## Make compliance and observability part of the comparison

Cost visibility is incomplete if the data path cannot be approved. Check regional availability, retention behavior, subprocessors, audit export, contractual requirements, and the organization's rules for sensitive marketplace content before a candidate enters production. A lower estimate does not override those constraints.

The observability design should answer three questions without reconstructing them from scattered logs: which tenant initiated the operation, what policy and model were used, and what happened to the result. Emit counters for input tokens, output tokens, retries, validation failures, review transfers, and accepted results. Keep high-cardinality tenant dimensions in a controlled usage store rather than making every application log an unbounded billing database.

Budget alerts should be tied to a baseline and a documented action. A sudden increase in input tokens can indicate a normalization change; a sudden increase in review rate can indicate a schema or model change; a sudden increase in retries can indicate capacity pressure. These are different investigations, and collapsing them into one "AI cost" alert loses the signal needed to act.

For regulated payment or ledger flows, preserve an exportable audit trail with access controls and retention matching the applicable policy. The extraction system may propose structured fields, but it should not silently mutate the system of record when required identifiers are absent or totals cannot reconcile.

## Roll out with a reversible decision gate

Start in shadow mode: count and estimate a representative sample, run shortlisted candidates against reviewed truth, and write no extracted value to the system of record. Compare results by tenant and document class, not only in aggregate. Promote a class only after its schema, field-accuracy, invariant, review, and governance thresholds are recorded with an owner and a policy version.

Then release realtime traffic behind a bounded gate and process batch work in small, reconcilable partitions. Keep candidate results and attempt metadata long enough to reproduce a model comparison. When normalization, schema, tokenizer, model, routing policy, or compliance assumptions change, rerun the evaluation instead of treating the old cost estimate as permanent.

This sequence keeps the decision honest: token counting limits exposure, a labeled corpus establishes reliability, tenant-level records explain allocation, and idempotent acceptance protects downstream books. The right architecture may reject a nominally cheaper option when its evidence, region, retention model, or latency behavior does not fit the marketplace contract. That is a limitation of the choice, not a failure of the accounting method.

## References

- https://github.com/openai/tiktoken
- https://openrouter.ai/docs
