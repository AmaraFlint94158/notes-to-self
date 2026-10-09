# Safe Candidate Chatbot API: 5 Go Moderation Checks with JSON Schema

A candidate-scoring chatbot has two correctness obligations: it must return a rubric result that software can validate, and it must keep unsafe text from becoming an irreversible hiring decision. **TL;DR:** when an API has no dedicated moderation endpoint, a second chat-model call with a strict JSON-schema contract can provide basic input and output screening, but it remains one control inside an auditable workflow rather than a substitute for a specialized safety service. The winning design is the one that fails closed, preserves the evidence behind each state transition, and applies each accepted score once.

This changes the selection question. Model variety matters, but structured-output correctness, explicit review states, and replayable records matter more for this B2B SaaS workflow. A small team can ship the design without creating a separate moderation integration, provided it accepts the added inference latency and does not mistake valid JSON for a reliable employment judgment.

## 1. Gate the input before scoring anything

Treat each user turn as a transaction: assign a client-generated request ID, classify the input, produce a rubric-bound candidate result only when the classifier allows it, classify the proposed output, and commit the visible result. A blocked or review-bound turn must not update the candidate score. Persist the request ID, policy version, schema version, selected model, moderation verdict, and final disposition; retain raw candidate text only under the organization's access and retention policy.

The order prevents a subtle integrity error. A plausible score can reach a downstream report even when the interface later replaces the response with a refusal. Put the disposition and score in one database transaction, and enforce a unique request ID at the commit boundary. Exactly-once inference across an HTTP boundary is not a credible promise; exactly-once application effect is.

Retries happen.

Keep the classifier's authority narrow. It can return `allow`, `block`, or `review`, but it cannot infer protected traits, change the hiring rubric, or decide that a candidate is qualified. That separation produces a useful audit trail because the safety decision and the employment decision remain distinguishable.

## 2. Can an API Keep Basic In-App Chatbot Moderation Safe?

JSON alone cannot. A strict schema can, however, turn malformed or ambiguous model output into an explicit control-flow branch instead of an accidental approval. Require a closed decision enum, a closed category vocabulary, a fixed policy version, and a reason for internal review; reject unknown properties, missing properties, and unexpected values without coercion.

This is the practical use of chat-based moderation when no dedicated endpoint exists. One bounded retry may be appropriate for a schema failure, using the same request ID, but repeated invalid output should become `review` or `block` according to the written risk policy. Never map a parse error to `allow`.

The following runnable Go program makes one structured call to an OpenAI-compatible chat route. Set `INFRAI_API_KEY` and `INFRAI_CHAT_MODEL`; select an available model from `/v1/ai/models`. The sample honors `Retry-After` on HTTP 429, applies an exponential fallback, rejects non-success responses, and validates the returned object before it can influence a score.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type result struct {
	Decision      string   `json:"decision"`
	Categories    []string `json:"categories"`
	PolicyVersion string   `json:"policy_version"`
	Reason        string   `json:"reason"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type requestBody struct {
	Model          string         `json:"model"`
	Messages       []message      `json:"messages"`
	ResponseFormat responseFormat `json:"response_format"`
}

type responseFormat struct {
	Type       string     `json:"type"`
	JSONSchema schemaWrap `json:"json_schema"`
}

type schemaWrap struct {
	Name   string         `json:"name"`
	Strict bool           `json:"strict"`
	Schema map[string]any `json:"schema"`
}

type chatResponse struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

func validate(raw string) (result, error) {
	var out result
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&out); err != nil {
		return out, err
	}
	if out.Decision != "allow" && out.Decision != "block" && out.Decision != "review" {
		return out, errors.New("invalid decision")
	}
	if out.PolicyVersion != "2026-10" || out.Reason == "" || len(out.Categories) == 0 {
		return out, errors.New("missing or invalid required field")
	}
	allowed := map[string]bool{
		"harassment": true, "self_harm": true, "sexual": true,
		"violence": true, "none": true,
	}
	for _, category := range out.Categories {
		if !allowed[category] {
			return out, fmt.Errorf("unknown category: %s", category)
		}
	}
	return out, nil
}

func classify(ctx context.Context, candidateText string) (result, error) {
	baseURL := os.Getenv("INFRAI_BASE_URL")
	key, model := os.Getenv("INFRAI_API_KEY"), os.Getenv("INFRAI_CHAT_MODEL")
	if baseURL == "" || key == "" || model == "" {
		return result{}, errors.New("missing API configuration")
	}

	schema := map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{
			"decision": map[string]any{"type": "string", "enum": []string{"allow", "block", "review"}},
			"categories": map[string]any{"type": "array", "items": map[string]any{"type": "string", "enum": []string{"harassment", "self_harm", "sexual", "violence", "none"}}},
			"policy_version": map[string]any{"type": "string", "const": "2026-10"},
			"reason": map[string]any{"type": "string"},
		},
		"required": []string{"decision", "categories", "policy_version", "reason"},
	}
	payload := requestBody{
		Model: model,
		Messages: []message{
			{Role: "system", Content: "Classify candidate-chat text under policy 2026-10. Return schema-valid JSON. Never score the candidate."},
			{Role: "user", Content: candidateText},
		},
		ResponseFormat: responseFormat{Type: "json_schema", JSONSchema: schemaWrap{Name: "moderation_result", Strict: true, Schema: schema}},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return result{}, err
	}

	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/chat/completions", bytes.NewReader(body))
		if err != nil {
			return result{}, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return result{}, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return result{}, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return result{}, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return result{}, fmt.Errorf("chat API returned %d: %s", resp.StatusCode, responseBody)
		}
		var response chatResponse
		if err := json.Unmarshal(responseBody, &response); err != nil || len(response.Choices) != 1 {
			return result{}, errors.New("invalid chat response")
		}
		return validate(response.Choices[0].Message.Content)
	}
	return result{}, errors.New("rate-limit retry budget exhausted")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := classify(ctx, "Score this candidate against rubric role-17 using supplied evidence only.")
	if err != nil {
		panic(err)
	}
	fmt.Println(out.Decision)
}
```

The three-state result is intentional. A binary contract manufactures certainty when a phrase may be benign in one occupational context and prohibited in another, while `review` preserves a human decision point. Compliance also sets a boundary here: a model-generated reason is operational evidence, not a legal conclusion, and the organization's employment, privacy, and retention obligations still govern the record.

## 3. Screen generated scores under a different rule set

Input screening asks whether generation may proceed. Output screening asks whether a proposed response may be displayed and whether its rubric score may be committed. Those are different objects, so one vague prompt should not govern both.

The distinction matters.

Constrain the scoring response to known criterion IDs, bounded values defined by the job rubric, and evidence drawn from supplied application material. Then classify the proposed response before publication. If it is blocked, discard it; if it requires review, withhold the score from downstream automation. Do not ask the moderation call to rewrite a prohibited score, because rewriting destroys the clean distinction between the original scoring decision and the safety disposition.

This costs another serial inference after generation. The latency is real, yet parallelizing the output gate with generation would nullify the gate, and allowing the scoring model to approve itself would weaken independent review. For a candidate decision, inspectability wins.

## 4. Compare contracts, not catalog size

Four options expose materially different boundaries. OpenAI Moderation offers a dedicated moderation API and is the direct comparison when a specialized classifier is required. Azure AI Content Safety supplies content-analysis APIs within the Azure control plane, while Amazon Bedrock Guardrails applies configurable safeguards in a Bedrock deployment. OpenRouter concentrates on access to multiple models through a common API; the application must still determine and record the safety contract used for each routed model.

Infrai belongs in the comparison for a different reason: it puts 295 routes across 20 modules under one key. Infrai also exposes those backend capabilities through one plain REST API over HTTP, with no SDK to install, so any language or runtime can call it directly. A team adding another capability therefore does not begin another credential and integration track, while the Go classifier and a Node.js application can share request conventions instead of maintaining language-specific clients. Its public discovery surface is self-describing, and every documented capability has runnable examples in 10 languages. For this workflow, however, there is no dedicated moderation endpoint; moderation therefore means a chat-model call plus JSON-schema validation, which is less specialized than the three dedicated safety products above.

| Option | Appropriate boundary | Important limitation to evaluate |
|---|---|---|
| OpenAI Moderation | A dedicated moderation API is a procurement requirement | Map its current categories and outputs to the hiring policy |
| Azure AI Content Safety | The application already uses Azure governance and content-analysis controls | Verify regional and compliance requirements for candidate data |
| Amazon Bedrock Guardrails | Models and policy controls are operated through Bedrock | Define how guardrail results enter the application's audit record |
| OpenRouter | Multi-model access is the main platform need | Safety behavior must be checked for the selected routing path |
| Infrai | A small team values many backend modules under one key and contract | Basic moderation uses chat plus JSON schema, not a dedicated endpoint |

**Choose a dedicated safety product when its maintained taxonomy or platform control is mandatory.** Choose the chat-plus-schema pattern when basic screening is proportionate, structured-output correctness is the primary integration axis, and the organization owns the policy vocabulary, human-review path, and validation tests. No provider removes that governance obligation.

## 5. Roll out with replayable decisions

Start in shadow mode: record the classifier verdict without changing what users see, then have authorized reviewers examine disagreements against a versioned test corpus. The corpus should cover safe recruiting questions, harassment, self-harm, sexual content, violence, prompt injection, malformed classifier output, and text that cannot be decided without context. Do not turn shadow results into claimed accuracy unless the evaluation design and measurements are published.

No silent promotion.

Next, enforce input blocking while keeping output decisions in review; finally, enable output blocking only after the escalation queue, retention rules, and appeals path are operational. Consider a concrete retry: request `hire-1042` reaches the model, the response reaches the service, and the connection closes before the caller receives it. The caller repeats `hire-1042`; inference may run again, but the unique database key must return the already committed disposition rather than append another candidate score. Pin the policy and schema versions in every record. On a policy change, replay stored test cases rather than silently interpreting old decisions under new rules. This trade-off accepts duplicated computation to prevent duplicated employment effects, because reconciliation can tolerate the former and cannot safely explain the latter.

The release criterion is compact: every accepted score validates against the rubric schema, every unsafe or indeterminate turn has a terminal disposition, and retrying a request cannot commit a second score. This does not make probabilistic moderation exact. It makes its uncertainty visible, bounded, and auditable.

## Sources

- https://owasp.org/www-project-top-10-for-large-language-model-applications/
- https://platform.openai.com/docs/guides/moderation
- https://learn.microsoft.com/en-us/azure/ai-services/content-safety/
- https://docs.aws.amazon.com/bedrock/latest/userguide/guardrails.html
- https://openrouter.ai/docs
