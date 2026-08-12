# Property Hiring LLM JSON Schema Triage: Node.js Missing Fields, Null Values, Enum Mismatch

Short answer: treat an LLM candidate score as an untrusted, versioned event; require a stable JSON shape, represent absent evidence with explicit null values, keep enums closed, and spend at most one bounded repair attempt before human review. For a property-management hiring workflow, that usually produces a better quality-versus-latency decision than allowing the prompt to improvise missing fields.

The practical constraint is easy to state and surprisingly easy to violate: a landlord or property manager needs a defensible comparison of candidates, while the hiring queue still needs an answer before the next reviewer opens the file. A score that arrives quickly but invents a certification is a quality failure. A perfect extraction that waits indefinitely is an operational failure. I would rather accept a review state than quietly turn either failure into a persisted decision.

## How should a Node.js extraction schema treat missing fields, null values, and enum mismatch?

Start by separating three questions that are often compressed into one prompt. Must the key exist? Is there evidence for its value? Can the application interpret every possible value? Those questions map to required properties, nullable types, and enums respectively. They are related, but they are not interchangeable.

Shape first.

Consider a rubric for a maintenance coordinator. The result may need `candidate_id`, `score`, `evidence`, and `recommendation`. `candidate_id` and `score` are structural requirements. `evidence` can be an empty array when the résumé contains no supporting passage, but a field such as `license_number` may need to be present as `null` when the source is silent. `recommendation` should be a closed vocabulary such as `advance`, `review`, or `reject` only if the downstream workflow has defined those states.

The prompt should say this plainly: do not omit required keys; use null when the source does not establish a nullable fact; return only permitted enum members; quote the source span for each scored criterion. An absent key and a null value must remain distinguishable in validation, even if both lead to review. Otherwise, an analyst cannot tell whether the document lacked evidence or the extractor violated the contract.

A placeholder is not harmless. `unknown`, an empty string, and `N/A` are ordinary strings unless the domain explicitly assigns them meaning. They also create ugly reconciliation cases when a later correction arrives. In a ledger-oriented backend, I keep the original source identity and schema version beside the extraction, because “the model said so” is not an audit trail.

Take one candidate record that says “licensed maintenance technician” but gives no license number. If the extractor drops `license_number`, the reviewer sees a malformed object and cannot tell whether the prompt forgot the field. If it returns `""`, a downstream filter may treat the candidate as having supplied a value, even though the source did not. If it returns `"unknown"`, a later system may accept that token as if it were part of the controlled vocabulary. The defensible result is a present nullable field with `null`, an evidence list that does not cite an absent claim, and a review state if the rubric requires the license before advancement. Now change the source so it names a license but the recommendation is `advance-fast`: the shape is complete, yet the enum is still invalid. Those are two different repairs, two different audit reasons, and potentially two different queues. Collapsing them into a generic “fix JSON” instruction makes the response shorter while making the hiring decision harder to defend.

## Where does quality become a latency decision?

Run validation in layers. First parse JSON. Next check shape and types. Then apply rubric rules: a score must be within the agreed range, every non-null score must cite evidence, and a recommendation must be compatible with the score threshold. A schema can catch an enum mismatch; it cannot establish that a résumé actually supports “five years of HVAC supervision.” That second claim needs evidence checks and, sometimes, a reviewer.

When validation fails, classify the failure before retrying. A missing field is a shape defect. A null where the rubric requires evidence is a quality defect. An enum mismatch is a vocabulary defect. Each deserves a recorded reason such as `E_MISSING_FIELD`, `E_NULL_NOT_ALLOWED`, or `E_ENUM`. Use the same source bytes and the same schema version for one repair request, then stop. Attempt 2 is enough for a bounded repair policy; more retries can lower observed latency only by moving uncertainty into an invisible loop.

The decision rule is not universal. If a property manager is triaging a large pool and the result is advisory, a short repair budget may be reasonable. If the score automatically denies an applicant access to an interview, latency should yield to review and a stronger evidence requirement. I'm not sure which document layouts will dominate a particular portfolio until the validation histogram is observed, and your mileage may vary; that uncertainty belongs in capacity planning, not in a looser acceptance rule.

The model path is not suitable when a small, fixed set of fields can be extracted deterministically from structured application forms; a parser or rules engine has a more predictable latency profile there. Conversely, rules alone are a poor fit for varied résumé prose. The right boundary is the one that keeps unsupported claims out of the hiring decision.

## A small contract checker for a Node.js extraction boundary

The production caller may be written in Node.js, but the boundary should be language-neutral. The following Go fixture shows the important behavior without assuming an SDK or a provider-specific structured-output feature: parse, enforce required keys, admit explicit nulls, reject unknown fields, and generate one precise repair instruction. The service can translate the same states into its Node.js validator and queue model.

```go
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type CandidateScore struct {
	CandidateID    *string  `json:"candidate_id"`
	Score          *float64 `json:"score"`
	Evidence       []string `json:"evidence"`
	Recommendation *string  `json:"recommendation"`
}

func validate(raw []byte) (CandidateScore, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return CandidateScore{}, fmt.Errorf("invalid JSON: %w", err)
	}

	allowed := map[string]bool{
		"candidate_id": true, "score": true,
		"evidence": true, "recommendation": true,
	}
	for name := range fields {
		if !allowed[name] {
			return CandidateScore{}, fmt.Errorf("unknown field %q", name)
		}
	}
	for _, name := range []string{"candidate_id", "score", "evidence", "recommendation"} {
		if _, ok := fields[name]; !ok {
			return CandidateScore{}, fmt.Errorf("E_MISSING_FIELD: %s", name)
		}
	}

	var result CandidateScore
	if err := json.Unmarshal(raw, &result); err != nil {
		return CandidateScore{}, fmt.Errorf("field type mismatch: %w", err)
	}
	if result.Score != nil && (*result.Score < 0 || *result.Score > 100) {
		return CandidateScore{}, errors.New("score must be between 0 and 100")
	}
	if result.Recommendation != nil {
		valid := map[string]bool{"advance": true, "review": true, "reject": true}
		if !valid[*result.Recommendation] {
			return CandidateScore{}, fmt.Errorf("E_ENUM: recommendation=%q", *result.Recommendation)
		}
	}
	for i, item := range result.Evidence {
		if strings.TrimSpace(item) == "" {
			return CandidateScore{}, fmt.Errorf("evidence[%d] cannot be blank", i)
		}
	}
	return result, nil
}

func repairInstruction(validationErr error) string {
	return fmt.Sprintf(
		"Return the same candidate extraction as JSON only. Keep every required key. " +
		"Use null only where the source has no evidence. Fix this validation error: %v",
		validationErr,
	)
}
```

There is a subtle point in the struct above. A pointer distinguishes a JSON null from a non-null scalar, but a pointer alone does not prove that the key was present; the raw-field pass supplies that structural check. This is why decoding directly into a convenient application object is insufficient. The validator must preserve the distinction before persistence.

The same boundary should attach a source digest, rubric version, model identifier, validation result, and attempt number to an audit event. Do not place résumé text in broad application logs merely to make debugging convenient. Privacy, retention, and access controls still apply to an extraction service. Exactly-once processing also comes from the write boundary: use a stable source-and-operation identity, make the accepted event idempotent, and reconcile accepted scores with review tasks rather than trusting a retry counter.

This is the part I would defend in a design review: a fast invalid answer is still invalid.

## What should teams measure before changing the extraction prompt?

Shadow a new prompt and schema against a governed sample before changing the live acceptance path. Measure missing-key rate, explicit-null rate, enum failures, evidence coverage, repair rate, p95 latency, and manual-review rate separately. A single “valid JSON” percentage conceals the failure that matters most to a hiring reviewer.

The comparison belongs after those measurements, not before them:

| Approach | Quality control | Latency profile | Best fit | Main limitation |
| --- | --- | --- | --- | --- |
| Rules-first extraction | Deterministic fields and explicit tests | Predictable | Structured application forms | Weak on varied résumé language |
| LLM extraction with validation | Evidence checks, schema checks, and review states | Variable; bounded by one repair | Mixed-format documents | Requires careful prompts, validators, and audit events |
| LLM extraction without validation | Little protection beyond parseability | Usually lowest apparent latency | Prototypes only | Unsafe for persisted hiring scores |
| Human review first | Highest contextual control | Queue-dependent | Consequential decisions and ambiguous evidence | Cost and throughput are limited by reviewer capacity |

No row wins every axis. A team that selects the lowest latency row for an automated recommendation may inherit the highest correction workload later; a team that sends every résumé to review may protect quality while missing its service-level target. The limitation is architectural, not a prompt defect.

Quality and latency should be plotted together. If a shorter prompt reduces latency but increases unsupported evidence, it is not an optimization; it is a transfer of work to reviewers. If a stricter rubric raises review volume, that may be the correct result for a consequential decision. Keep the old schema available for replay, and make rollout reversible by version rather than by editing a prompt in place.

Start small. One property portfolio, one rubric version, and one review queue make the reconciliation story legible. Once the outcomes are stable, expand the sample and test adversarial documents: missing licenses, conflicting dates, copied boilerplate, and candidates whose qualifications are present but phrased unexpectedly. The system should fail visibly into review, never silently into a fabricated field.

## Sources

- https://platform.openai.com/docs/guides/embeddings
- https://elevenlabs.io/docs
