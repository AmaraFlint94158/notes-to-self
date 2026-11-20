# How to Audit Logistics DNS Record Writes Before Default Upsert

Choose upsert as the default provisioning intent only for records the console owns, and require separate evidence before declaring a logistics sender ready. A successful mutation closes the configuration task; it does not establish that shipment notifications reached their recipients.

Short answer: use guarded upsert for repeatable DNS record writes, create for an absence requirement, and update for an existence requirement. For a Node.js provisioning caller, those are application contracts, independent of the runtime. The executable Go model below makes the contracts explicit without assuming that a DNS service exposes an atomic upsert operation.

## Should DNS record writes default to upsert without delivery evidence?

The internal admin console in this design manages a logistics sender domain used for dispatch notifications. Its decision record has one central requirement: an operator must be able to distinguish an accepted configuration change from evidence about messages using that configuration, because otherwise the same green indicator can conceal several materially different states. A carrier onboarding screen should not turn a mutation acknowledgment into a claim about delivery.

RFC 7489 supplies the relevant limit. DMARC evaluates identifier alignment and authentication results, and its aggregate feedback can describe message populations seen by reporting receivers. That feedback does not prove inbox placement or receipt of a particular shipment notification. Section 6.7 leaves final message disposition to local receiver policy. The proposed console records configuration status, authentication evidence, and delivery evidence separately; its release decision requires an explicit operator judgment about which evidence is sufficient for the intended notification workflow.

Evidence has scope.

An aggregate report covering an earlier observation interval cannot establish the effect of a later DNS change. Even a report relevant to the current revision should support only the claim its contents justify. In this design, authentication observations can advance an authentication review, while delivery readiness remains pending until a separate, attributable delivery observation is attached. No supplied protocol source defines the logistics business's delivery acceptance threshold, so that threshold must be an explicit local policy rather than a purported DMARC guarantee.

## Record the invariants before selecting the write operation

The proposed identity of a managed record set is the tenant, zone, owner name, and record type. Ownership belongs to that identity, not to the button that triggered the write. An idempotency key identifies one requested change and its complete intent; reusing the key for different content is a conflict. A revision identifies the state against which the change was authorized. These are design choices for the console, not claims about a particular DNS API.

Preserve both intent and outcome in an audit trail. The target is exactly-once intent accounting: one accepted request has one stable result when retried, although an external DNS mutation and a local database transaction do not thereby become one atomic operation. A timeout leaves an uncertain outcome that needs reconciliation. It does not authorize a second, differently scoped write.

| Operation contract | Preconditions in this design | Appropriate use | Failure boundary |
| --- | --- | --- | --- |
| Create | Console owns the identity and no managed state exists | First enrollment when unexpected state must stop approval | Existing state is a conflict, even if its content matches |
| Update | Console owns the identity and the expected revision exists | Approved changes to an enrolled sender | Missing or changed state requires review |
| Guarded upsert | Console owns the identity and the expected revision matches, including revision zero for absence | Repeated desired-state provisioning | A retry may replay its result; a new request cannot overwrite a stale revision |

For DMARC specifically, section 6.6.3 describes policy discovery and requires processing to stop when multiple applicable records remain after filtering. That makes blind insertion a concrete trap: a second policy record is not a harmless way to express a newer policy. Treat the policy as one managed desired state, and stop for review when discovery finds ambiguous or independently administered state. The fixture below represents the authorized policy payload; production discovery must establish the ownership boundary before this model is applicable.

## Step 1 Implement the guarded provisioning transaction

Save this complete program as `main.go` and run it with `go run main.go`. Its fixture models `_dmarc.dispatch.example`, a logistics sender under a reserved example domain, with a policy of `v=DMARC1; p=none`. Section 6.3 defines `none` as requesting no specific action; choosing it here exercises provisioning without presenting enforcement as delivery evidence.

The program uses a mutex to make its in-memory state, replay ledger, and audit append atomic within one process. Its checks demonstrate the critical path: apply an authorized intent, replay an identical request, reject key reuse with different content, reject a stale revision, and reject an unowned identity. The final check expects one committed mutation and one audit event. Failed attempts would need a separate attempt log in a deployed console.

```go
package main

import (
	"fmt"
	"sync"
)

type Identity struct {
	Tenant, Zone, Name, Type string
}

type Intent struct {
	ID     Identity
	Key    string
	Mode   string
	Value  string
	Expect uint64
}

type State struct {
	Value    string
	Revision uint64
}

type Receipt struct {
	Request Intent
	Result  State
}

type ReplayKey struct {
	Tenant, Key string
}

type Store struct {
	mu       sync.Mutex
	owned    map[Identity]bool
	records  map[Identity]State
	receipts map[ReplayKey]Receipt
	audit    []Receipt
}

func (s *Store) Apply(in Intent) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.Key == "" || in.Value == "" {
		return State{}, fmt.Errorf("missing request key or desired value")
	}
	if !s.owned[in.ID] {
		return State{}, fmt.Errorf("identity is outside managed ownership")
	}
	rk := ReplayKey{Tenant: in.ID.Tenant, Key: in.Key}
	if prior, ok := s.receipts[rk]; ok {
		if prior.Request != in {
			return State{}, fmt.Errorf("request key reused for different intent")
		}
		return prior.Result, nil
	}
	current, exists := s.records[in.ID]
	switch in.Mode {
	case "create":
		if exists {
			return State{}, fmt.Errorf("create requires absence")
		}
	case "update":
		if !exists {
			return State{}, fmt.Errorf("update requires existing state")
		}
	case "upsert":
	default:
		return State{}, fmt.Errorf("unknown operation")
	}
	if current.Revision != in.Expect {
		return State{}, fmt.Errorf("revision conflict")
	}
	result := current
	if !exists || current.Value != in.Value {
		result = State{Value: in.Value, Revision: current.Revision + 1}
	}
	receipt := Receipt{Request: in, Result: result}
	s.records[in.ID] = result
	s.receipts[rk] = receipt
	s.audit = append(s.audit, receipt)
	return result, nil
}

func main() {
	id := Identity{"carrier-a", "dispatch.example", "_dmarc.dispatch.example", "TXT"}
	s := Store{
		owned:    map[Identity]bool{id: true},
		records:  make(map[Identity]State),
		receipts: make(map[ReplayKey]Receipt),
	}
	in := Intent{ID: id, Key: "sender-enrollment-1", Mode: "upsert",
		Value: "v=DMARC1; p=none", Expect: 0}
	first, err := s.Apply(in)
	if err != nil {
		panic(err)
	}
	replay, err := s.Apply(in)
	if err != nil || replay != first {
		panic("retry did not return the original result")
	}
	changed := in
	changed.Value = "v=DMARC1; p=reject"
	if _, err := s.Apply(changed); err == nil {
		panic("different intent reused a request key")
	}
	stale := changed
	stale.Key = "sender-change-2"
	if _, err := s.Apply(stale); err == nil {
		panic("stale revision was accepted")
	}
	unowned := in
	unowned.Key = "other-carrier-1"
	unowned.ID.Tenant = "carrier-b"
	if _, err := s.Apply(unowned); err == nil {
		panic("ownership check was bypassed")
	}
	if first.Revision != 1 || len(s.audit) != 1 {
		panic("unexpected mutation or audit count")
	}
	fmt.Println("revision", first.Revision, "committed audit events", len(s.audit))
}
```

This is an executable transaction model, not an external DNS client. Its limitations include process-local atomicity and volatile storage; it is not suitable for multiple workers or production persistence without a durable transaction boundary. It deliberately omits network mutation so that its atomicity claim stays bounded by the mutex. A deployment needs durable storage, authenticated tenant authorization, record discovery, input validation, and coordination among workers; an in-memory ownership map cannot supply those controls. The single policy string also avoids pretending to implement general DNS record-set normalization.

## Step 2 Keep authentication observations separate from delivery approval

Save the following independent program as `evidence.go` in a separate directory and run `go run evidence.go`. These synthetic observations show the console's proposed evidence contract, not a DMARC report schema. A reviewer must establish that an observation pertains to the revision before assigning that revision in this model; a report does not carry the console's revision number automatically.

```go
package main

import "fmt"

type Observation struct {
	Revision        uint64
	Source          string
	Authentication  bool
	DeliveryReceipt bool
}

func releaseStatus(revision uint64, observations []Observation) string {
	authentication, delivery := false, false
	for _, observation := range observations {
		if observation.Revision != revision || observation.Source == "" {
			continue
		}
		authentication = authentication || observation.Authentication
		delivery = delivery || observation.DeliveryReceipt
	}
	if authentication && delivery {
		return "eligible for operator review"
	}
	return "pending evidence"
}

func main() {
	observations := []Observation{
		{Revision: 1, Source: "reviewed aggregate report", Authentication: true},
		{Revision: 0, Source: "earlier delivery observation", DeliveryReceipt: true},
	}
	if releaseStatus(1, observations) != "pending evidence" {
		panic("old delivery evidence released a new revision")
	}
	observations = append(observations, Observation{
		Revision: 1, Source: "reviewed notification receipt", DeliveryReceipt: true,
	})
	if releaseStatus(1, observations) != "eligible for operator review" {
		panic("current evidence did not advance review")
	}
	fmt.Println(releaseStatus(1, observations))
}
```

The distinction costs operator attention, but it preserves the reason for the release decision. In a real workflow, the evidence record should identify the sender, observation interval, message population or notification identifier, collection source, and reviewer, because a bare Boolean would erase the information needed for reconciliation. The example only checks the revision and source to make the acceptance boundary executable; it does not verify receipts or authenticate report senders.

Nor does this establish compliance. RFC 7489 section 9 discusses privacy considerations, including reporting information; it does not certify a console, prescribe a universal retention period, or satisfy an organization's access-control obligations. Keep evidence access and retention under explicit organizational policy. Store only what the audit purpose requires.

## Step 3 Reconcile uncertain outcomes before enabling retries

The two runnable programs establish the local acceptance criteria: retries preserve the receipt, conflicting intents fail, and stale evidence cannot advance review. Use those criteria as deployment gates before connecting the transaction model to an external writer. The difficult case is a write that might have succeeded before the response was lost, because a worker that treats an exception as proof of absence can duplicate intent or overwrite a subsequent operator change. Persist the authorized intent before dispatch, and reconcile observed state against that intent after an uncertain response; where the adapter cannot distinguish competing changes safely, preserve uncertainty and require review.

Do not guess success.

For an initial rollout, propose a limited set of explicitly enrolled sender identities and retain manual release approval. Operational visibility should expose unresolved write outcomes, revision conflicts, and evidence awaiting review as separate queues, with the same tenant and request identifiers used by the audit trail. Resource expenditure comes from reconciliation work, retained evidence, and reviewer time as well as mutations; no unit-price comparison answers whether the console can justify a particular sender's release.

The rejected default is unconditional create on every provisioning attempt. Its valid use case is enrollment where unexpected existing state is itself a reason to stop, and an idempotency receipt can distinguish a replay from a new enrollment request. Update-only is similarly appropriate for an approved change to an already enrolled identity. For recurring provisioning, guarded upsert provides the desired convergence contract, while the separate evidence decision preserves the limit that matters to logistics operations: configuration acceptance alone cannot substantiate notification delivery.

## References

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance, especially sections 6.3, 6.6.3, 6.7, 7.2, and 9: https://datatracker.ietf.org/doc/html/rfc7489
