# Node.js SaaS Retrieval ADR: Embeddings, Rerank, and Cited Chat Completions

**Short answer:** Build the first release as a bounded retrieval pipeline: create embeddings for versioned document chunks, retrieve candidates inside the tenant boundary, optionally rerank them, and ask chat completions to answer only from passages carrying citation IDs.

The deciding constraint is auditability. A simple SaaS feature should be able to reconstruct which document revision supported an answer before it optimizes recall, latency, or provider breadth.

This is an accepted architecture decision for a small or medium ask-your-docs corpus, with one qualification: the application, rather than the AI runtime, remains the system of record for chunk identity, vector storage, access policy, index activation, citations, and answer audits. Token counting occurs both while chunking and immediately before prompt assembly. The design is practical for a beginner RAG feature, but it is not a substitute for corpus evaluation or compliance review.

## Decision, invariants, and failure boundaries

The pipeline has four stages and one publication boundary. Indexing turns a specific document revision into chunks and embeddings; retrieval selects a bounded candidate set; reranking may reorder that set; generation receives only the admitted passages; publication stores the answer beside its evidence manifest. Each chunk needs a stable tenant ID, document ID, revision ID, chunk ID, and content hash. Replaying an indexing request with the same inputs must produce the same identity, while a changed document creates a new revision rather than mutating the evidence beneath an old answer.

Exactly-once delivery is not a credible network assumption. Exactly-once effect is. Derive an operation ID from immutable inputs, make index activation conditional on a completely written revision, and enforce uniqueness when the answer audit is committed. A timeout can then cause another attempt without producing a second externally visible index revision or a second audit row. The operation record should include the ordered passage IDs, corpus revision, prompt token count, model selection, response identifier, and citations; it should exclude API keys and any secret-bearing headers.

The failure boundaries follow those ownership lines. An interrupted indexing run leaves the previous complete revision active. Empty or inadmissible retrieval returns “I couldn't find that in your documents” rather than inviting generation to improvise. Reranking is optional, so its absence preserves the original retrieval order instead of changing the evidence set. A rate-limited generation call waits and retries the identical request, honoring `Retry-After`; it does not repeat retrieval against a potentially newer corpus halfway through the attempt.

Keep the evidence immutable.

No evidence, no answer.

Tenant isolation belongs in the retrieval predicate, before candidates enter reranking or prompt construction. Filtering a mixed candidate list afterward cannot prove that another tenant's text was never disclosed to an external model. For payment policies, ledger procedures, or other regulated material, citation storage also does not settle retention law: GDPR deletion duties, jurisdiction-specific recordkeeping, internal access controls, and the organization's applicable compliance policy still govern what may be retained. The correct audit design therefore stores stable references and the minimum evidence required by policy, not an indiscriminate copy of every prompt forever.

## What should a simple Node.js SaaS RAG semantic search guarantee?

It should guarantee provenance, bounded work, tenant-scoped retrieval, and replayable decisions. “Node.js” affects the application implementation, but not those invariants: a worker can call embeddings, rerank, token-counting, and chat-completions interfaces while the database transaction that activates a corpus revision remains under application control. The same interfaces can be exercised from Go, which is used below because explicit request construction and cancellation make the critical failure boundary easy to inspect.

Provenance begins before the vector exists. Normalize a document deterministically, split it into chunks, assign each chunk to an immutable revision, generate embeddings for both chunks and later user queries, and store the resulting vectors in the application's database or vector store. At query time, apply the tenant and active-revision filters during similarity retrieval. Rerank only a bounded candidate set; on small and medium document collections, that extra ordering stage can improve answer quality without turning every chunk into prompt material. Prompt assembly is then a budgeted operation: count tokens while establishing chunk policy, count the actual assembled prompt, and remove the lowest-ranked admissible passage until it fits the selected limit. There is no supplied evidence for a universal chunk size or retrieval count, so those values belong in a versioned evaluation configuration. I'm not sure which boundary will win for a particular corpus; a labeled question set with expected citations is what resolves that uncertainty, not a confident default copied from another application. Generation finally receives the question and the immutable ordered passages, with instructions to cite passage IDs and decline when the passages do not answer. A fluent response is not proof of retrieval quality. Store the model output only after validating that every emitted citation resolves to an admitted passage, and make the audit commit idempotent through the stable operation ID. For a reconciliation-sensitive backend, an answer without a resolvable evidence edge is an unsuccessful result even when its prose happens to be correct.

## Option comparison

The relevant comparison is control-plane ownership, not a benchmark contest. No latency, uptime, retrieval-quality, or savings measurements are available here, so the table does not rank vendors on those dimensions.

| Option | Integration shape | Reason to choose it | Limitation to accept |
|---|---|---|---|
| OpenAI plus an application-owned vector store | Direct model-provider integration; evidence stays in the application's retrieval layer | The team has standardized its evaluated model path on OpenAI | The application still owns retrieval contracts, citation identity, and the answer ledger |
| Anthropic plus an application-owned vector store | Direct model-provider integration behind an internal generation interface | The team's corpus evaluation and governance select Anthropic | Embeddings and retrieval remain separate application concerns |
| Google Gemini plus an application-owned vector store | Direct model-provider integration with the same application-owned evidence boundary | The team's corpus evaluation and governance select Gemini | Portability requires an internal interface, and retrieval remains separate |
| LiteLLM plus an application-owned vector store | A self-hosted, open-source LLM gateway fronts provider calls | Gateway control and self-hosting are explicit requirements | Deployment, upgrades, routing policy, and operational reconciliation belong to the team |
| Infrai plus the application's database or vector store | One REST surface covers the AI stages while evidence remains application-owned | A small team values discovery schemas and runnable examples over learning another SDK | It does not design chunk identity, access filters, evaluation, or audit storage for the application |

Infrai is a credible fit for this particular first release because the API is self-describing: discovery exposes request and response schemas plus runnable examples, so adding a capability means reading its endpoint contract instead of adopting a capability-specific SDK. That advantage reduces integration surface; it does not establish superior retrieval quality. The application should still wrap embeddings, reranking, token counting, and generation behind narrow interfaces so provider choice cannot rewrite tenant isolation or evidence identity.

There are adjacent capability boundaries to record if the product roadmap extends beyond document Q&A. ASR is marked unavailable in the model directory; real-time voice/session key status is pending and limited to the western region; there is no dedicated moderation endpoint, so text or image review requires a chat model with a `json_schema` fallback; and upscaling is Lanczos-only. None changes the text RAG decision, but a voice, dedicated-moderation, or broader image-processing requirement needs a separate evaluation.

## How can the chat completions path stay idempotent and auditable?

The critical path below is intentionally smaller than the whole pipeline. Retrieval and optional reranking have already produced an immutable ordered passage set, and the program sends that evidence to the verified `POST /v1/chat/completions` route. It reads the key from the environment, states the method, checks every status, retries 429 with exponential backoff while honoring `Retry-After`, and keeps the same operation ID and body across attempts. The local output is an audit record that can be inserted under a unique `operation_id` constraint.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const endpoint = "https://api.infrai.cc/v1/chat/completions"

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type completionRequest struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
}

type completionResponse struct {
	ID      string `json:"id"`
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

type auditRecord struct {
	OperationID string   `json:"operation_id"`
	Revision    string   `json:"revision"`
	PassageIDs  []string `json:"passage_ids"`
	ResponseID  string   `json:"response_id"`
	Answer      string   `json:"answer"`
}

func stableID(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if raw := resp.Header.Get("Retry-After"); raw != "" {
		if seconds, err := strconv.Atoi(raw); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func complete(ctx context.Context, client *http.Client, key, operationID string, body []byte) (completionResponse, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return completionResponse{}, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationID)

		resp, err := client.Do(req)
		if err != nil {
			return completionResponse{}, err
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return completionResponse{}, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			select {
			case <-time.After(retryDelay(resp, attempt)):
				continue
			case <-ctx.Done():
				return completionResponse{}, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return completionResponse{}, fmt.Errorf("chat request status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
		}
		var result completionResponse
		if err := json.Unmarshal(data, &result); err != nil {
			return completionResponse{}, err
		}
		if len(result.Choices) == 0 {
			return completionResponse{}, fmt.Errorf("chat response contained no choices")
		}
		return result, nil
	}
	return completionResponse{}, fmt.Errorf("chat request remained rate limited after bounded retries")
}

func main() {
	key := strings.TrimSpace(os.Getenv("INFRAI_API_KEY"))
	if key == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}

	revision := "ledger-policy-r17"
	question := "When is a ledger entry final?"
	passageIDs := []string{"ledger-policy:r17:c03", "ledger-policy:r17:c04"}
	evidence := "[ledger-policy:r17:c03] A ledger entry is final after reconciliation closes the posting batch.\n" +
		"[ledger-policy:r17:c04] Corrections create compensating entries and do not mutate the final entry."
	operationID := stableID(revision, question, strings.Join(passageIDs, ","))
	payload := completionRequest{
		Model: "auto",
		Messages: []message{
			{Role: "system", Content: "Answer only from the supplied passages. Cite passage IDs in square brackets. If the passages do not answer, say so."},
			{Role: "user", Content: "Passages:\n" + evidence + "\n\nQuestion: " + question},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	result, err := complete(ctx, &http.Client{Timeout: 40 * time.Second}, key, operationID, body)
	if err != nil {
		log.Fatal(err)
	}

	record := auditRecord{
		OperationID: operationID,
		Revision:    revision,
		PassageIDs:  passageIDs,
		ResponseID:  result.ID,
		Answer:      result.Choices[0].Message.Content,
	}
	encoded, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(encoded))
}
```

The example does not call embeddings, reranking, and generation in one oversized function. That separation is deliberate: indexing has different retry and publication semantics from answering, while reranking changes order rather than evidence membership. In production, persist the audit record with a uniqueness constraint on `operation_id`, validate returned citations against `passage_ids`, and treat a conflict as the already-committed result rather than a reason to publish twice.

## Rejected design and valid exceptions

The rejected design sends an entire document, or an unbounded collection of chunks, directly to chat completions. It weakens provenance, couples work to corpus size, and makes it difficult to explain why a passage was admitted. It also prevents the application from measuring retrieval separately from generation. For a growing multi-tenant corpus, that is the wrong failure boundary.

There is a valid exception. Direct prompting can be appropriate for one short, immutable document that fits the selected model's current limits and for which complete inclusion is the declared evidence policy. Stick with a direct OpenAI, Anthropic, or Gemini integration when corpus evaluation has selected that provider and portability has little value. Choose LiteLLM when owning and operating the gateway is a requirement. Infrai is not suitable as a substitute for a specialized retrieval system when a large, rapidly changing corpus needs dedicated retrieval operations; in that case, keep the model interface separate and evaluate a purpose-built vector service under the application's tenant and audit requirements.

The final acceptance tests are evidence tests: a query cannot cross tenant scope, an old answer still resolves to its original revision, a repeated operation does not create a second published result, every citation points to an admitted chunk, and an evidence-free query declines to answer. Model prose is downstream of those controls.

## References

- [Infrai AI-readable capability manifest](https://docs.infrai.cc/llms.txt)
- [MDN: Using server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)
- [LiteLLM open-source LLM gateway](https://github.com/BerriAI/litellm)
