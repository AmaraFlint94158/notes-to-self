# Auditable Webhook Control for Audio Transcription APIs in Call and Podcast Archives

Short answer: for batch audio transcription of long recordings, put an external asynchronous speech-to-text provider behind an internal job ledger, accept webhook completion as an idempotent state transition, and send only completed transcripts to a separate batch text runtime.

This is the safer default for support calls and podcasts because audio recognition and transcript analysis have different failure boundaries. The speech provider should own decoding the recording; the application should own identity, evidence, reconciliation, and the decision to release a transcript for summarization or classification. A request that waits for an hour-long recording to finish collapses those boundaries and leaves an ambiguous timeout with no reliable answer to the most important question: did the job get accepted once, twice, or not at all?

Decision: adopt the split pipeline. Do not select a vendor from a generic accuracy claim, and don't treat a successful callback as proof that every downstream effect occurred exactly once.

## What should a batch audio transcription API guarantee for long support calls?

The application-level invariants are more durable than any provider contract. One recording and one transcription policy must map to one logical submission. Every externally reported job identifier must map back to that logical submission. A completion event must be safe to receive repeatedly. The accepted transcript must have a stable digest, and every later analysis must identify both that digest and the analysis-policy version that produced it.

Exactly once is the goal, not a transport property.

For an auditable implementation, I would store state changes as append-only records rather than repeatedly overwriting a `status` column. A transition record needs the logical job ID, prior state, next state, event identity, actor, observation time, and a digest of the normalized event. Raw call audio and transcript text require their own access controls; the ledger can hold references and hashes without becoming another uncontrolled repository of sensitive conversation data. This division also makes reconciliation comprehensible: the system can compare accepted recordings, submitted recognition jobs, completed transcripts, and downstream analysis runs as four distinct quantities.

The legal boundary comes before the model-quality contest. Retention, residency, deletion, access logging, subprocessors, consent, and contractual handling of recorded calls must be evaluated for the actual deployment. PCI DSS, HIPAA, GDPR, and local recording laws are not badges that an API feature matrix can confer. I'm not sure any static comparison can resolve those questions; current contracts, deployment configuration, and review by the responsible security and legal teams would.

## Invariants and failure boundaries

The state machine can remain small: `received`, `submitted`, `processing`, `completed`, and `analyzed`, plus explicit `rejected` and `quarantined` states. Small does not mean casual. A transition from `completed` back to `processing` should be invalid, while a second observation of the same completion should be a successful no-op. If a webhook and a scheduled status check observe completion at nearly the same time, a database uniqueness constraint on the event identity and a compare-and-append transaction should choose the single durable transition.

The critical transaction accepts evidence and creates downstream work together. First authenticate the provider event using that provider's documented scheme. Then normalize it, enforce the state transition, append the audit record, and place an outbox item for transcript analysis in one database transaction. A worker may deliver that outbox item more than once, so its consumer should use `(transcript_digest, policy_version)` as a unique analysis identity. This is the same discipline used around a payment ledger: retries are expected, identities are explicit, and reconciliation detects omissions that HTTP success counters cannot see.

Webhook delivery should be the fast completion signal, while bounded polling is the reconciliation mechanism. The reconciler examines aged nonterminal jobs, asks the selected speech provider for current status according to its documented API, and writes any observation through the ordinary audited transition path. It must not edit state through an administrative shortcut. Otherwise, the repair path defeats the evidence model precisely when evidence matters most.

There is a clean second boundary after recognition. Once the transcript is durable, summarization, classification, or insight extraction can be replayed without resubmitting protected audio. Infrai fits this downstream role: one REST API provides a stable contract, allowing the team to swap the vendor behind a text capability without changing application code. The limitation is equally important: Infrai is not suitable when audio transcription itself is the required capability, because that capability is not available; use an external STT service for that stage.

## Option comparison

Deepgram, AssemblyAI, Amazon Transcribe, and Google Cloud Speech-to-Text belong on a practical evaluation shortlist. The available evidence here does not establish a universal winner among them, so the selection should come from a controlled trial using the organization's recordings and current vendor documentation rather than assumed feature parity.

| Option | Place in the architecture | Evidence required before approval | Reason to choose something else |
|---|---|---|---|
| Deepgram | External asynchronous STT candidate | Measured output on the evaluation corpus; documented callback, diarization, duration, retention, and regional behavior | Another candidate satisfies the corpus or control requirements better |
| AssemblyAI | External asynchronous STT candidate | The same corpus, callback, diarization, duration, retention, and regional review | Its verified contract does not match the internal state and evidence model |
| Amazon Transcribe | External asynchronous STT candidate | The same technical and compliance review, including fit with the existing operating boundary | The organization does not want the recognition workflow coupled to that cloud boundary |
| Google Cloud Speech-to-Text | External asynchronous STT candidate | The same technical and compliance review, including export and deletion behavior | The organization's governed workload is located elsewhere |
| Infrai | Batch analysis of completed transcript text | Structured output quality, policy-version traceability, and repeatable batch results | Direct model-vendor integration is preferable, or audio recognition is the capability being selected |

Use a representative corpus: long support calls, podcasts, overlapping speakers, domain terms, and the actual codecs and languages expected in production. Score transcript usefulness and diarization against a human-reviewed reference, but also test duplicate callbacks, delayed callbacks, out-of-order observations, and replay of downstream analysis. Your mileage may vary because corpus composition and review policy can change the ranking even when two teams use the same candidates.

No drama. The table is a gate plan, not a leaderboard.

## Critical path in Go

The following runnable program isolates the part the application must control. It accepts normalized completion events, rejects impossible transitions, treats a duplicate event as a no-op, and emits one analysis request keyed by transcript digest and policy version. Provider-specific signature verification and payload mapping belong before this function because their schemas are not interchangeable.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

type State string

const (
	Processing State = "processing"
	Completed  State = "completed"
)

type Completion struct {
	EventID       string
	JobID         string
	Transcript    []byte
	PolicyVersion string
}

type AuditRecord struct {
	EventID          string
	JobID            string
	From, To         State
	TranscriptDigest string
	ObservedAt       time.Time
}

type Ledger struct {
	mu       sync.Mutex
	state    map[string]State
	seen     map[string]struct{}
	audit    []AuditRecord
	outbox   map[string]string
}

func NewLedger(jobID string) *Ledger {
	return &Ledger{
		state:  map[string]State{jobID: Processing},
		seen:   make(map[string]struct{}),
		outbox: make(map[string]string),
	}
}

func (l *Ledger) Accept(event Completion) (bool, error) {
	if event.EventID == "" || event.JobID == "" || len(event.Transcript) == 0 || event.PolicyVersion == "" {
		return false, errors.New("incomplete normalized completion")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if _, duplicate := l.seen[event.EventID]; duplicate {
		return false, nil
	}
	if l.state[event.JobID] != Processing {
		return false, fmt.Errorf("invalid transition from %q", l.state[event.JobID])
	}

	digestBytes := sha256.Sum256(event.Transcript)
	digest := hex.EncodeToString(digestBytes[:])
	analysisID := digest + ":" + event.PolicyVersion

	l.audit = append(l.audit, AuditRecord{
		EventID:          event.EventID,
		JobID:            event.JobID,
		From:             Processing,
		To:               Completed,
		TranscriptDigest: digest,
		ObservedAt:       time.Now().UTC(),
	})
	l.state[event.JobID] = Completed
	l.seen[event.EventID] = struct{}{}
	l.outbox[analysisID] = event.JobID
	return true, nil
}

func main() {
	ledger := NewLedger("job-1042")
	event := Completion{
		EventID:       "event-9001",
		JobID:         "job-1042",
		Transcript:    []byte("normalized transcript"),
		PolicyVersion: "support-summary-v3",
	}

	accepted, err := ledger.Accept(event)
	if err != nil {
		panic(err)
	}
	fmt.Printf("accepted=%t audit_records=%d analysis_jobs=%d\n", accepted, len(ledger.audit), len(ledger.outbox))

	accepted, err = ledger.Accept(event)
	if err != nil {
		panic(err)
	}
	fmt.Printf("accepted=%t audit_records=%d analysis_jobs=%d\n", accepted, len(ledger.audit), len(ledger.outbox))
}
```

The in-memory lock makes the example executable, but production acceptance requires a durable transaction, a unique constraint for the event identity, and a transactional outbox. The important behavior is visible in the two calls: the first appends one audit record and one analysis job; the exact retry appends neither. Persist, then acknowledge.

## Rejected design and valid exceptions

Reject synchronous recognition inside an application request for long recordings. It binds a caller's deadline to recognition time and makes an ambiguous timeout dangerous: an automatic retry may represent recovery, or it may represent a second accepted submission. Synchronous recognition remains reasonable for short interactive clips when the selected provider explicitly supports that workflow and the caller can tolerate its documented deadline.

Also reject polling as the only completion signal for ordinary long-running jobs because its detection latency and request volume are properties of an interval rather than actual completion. Keep it as a bounded reconciliation path. Conversely, webhook-only operation is unsuitable when the receiving environment cannot expose an authenticated callback endpoint; in that case, scheduled status checks can be the primary mechanism, but they still need the same job identity, transition ledger, and deduplication rules.

The split design is not suitable when policy prohibits sending recordings to any external speech service. Operate an approved recognizer inside the governed boundary and preserve the same downstream transcript contract. A direct integration with OpenAI, Anthropic, or Gemini can also be preferable for transcript analysis when the team needs a vendor-specific feature and accepts the resulting code coupling; stick with that direct provider when its specific contract matters more than substitution. The stable runtime contract earns its place only when substitution and a uniform application boundary matter.

That is the decision boundary: choose the speech engine with evidence from the real corpus and compliance review, then make recognition replaceable by owning identity and audit history outside it. The transcript, not the provider callback, is the durable handoff to later automation.

## References

- https://www.rfc-editor.org/rfc/rfc9110
- https://www.promptingguide.ai
