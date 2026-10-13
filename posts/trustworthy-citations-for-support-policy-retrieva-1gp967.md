# Trustworthy Citations for Support Policy Retrieval in 2026 (Metadata Before Generation)

**Short answer:** assemble citations from retrieval metadata, then let the model write only the answer. A citation produced by a model is a formatting convention that sometimes happens to be true; a citation assembled from metadata is constrained to records the system actually retrieved. For customer-support teams detecting semantically near-duplicate compliance-policy records, that distinction determines whether a reviewer can reproduce an answer after the policy corpus changes.

The architecture decision is therefore to preserve immutable document and chunk identifiers, policy version, effective dates, and a content digest beside each vector; apply freshness rules before generation; and render the bibliography in application code from the exact returned set. Never accept a source identifier invented by the model. **If a regulator may read the output, only the metadata path is defensible.**

## What must remain true?

Four invariants govern this decision. A displayed citation cannot name a document absent from retrieval. Every citation resolves to the policy version supplied to generation. Replaying a stored retrieval snapshot yields the same citation set. Finally, an audit event connects the answer, query, retrieved chunk identifiers, and policy versions without trusting prose emitted by the model.

The failure boundary should be narrow: retrieval chooses evidence, deterministic application code creates citations, and generation summarizes evidence. The model may still emit an unsupported claim, so claim verification remains a separate control; generation may not expand the evidence set. Consider a revision race. A support record matches chunk 3 of policy version `2026.09`; before generation finishes, version `2026.10` becomes effective and the mutable parent record changes. An audit event containing only a friendly policy URL cannot establish which text the answer saw. One containing the retrieval snapshot's document ID, chunk ID, version, effective time, and digest can be reconciled after the current policy changes.

Provenance cannot vary.

Freshness is part of correctness, especially during duplicate detection. Two chunks can be semantically close while representing different policy revisions, and merging them merely because their language is similar can revive a superseded obligation. Store `effective_at`, `superseded_at`, `document_id`, `version`, `chunk_id`, and a digest with the vector, then exclude not-yet-effective and superseded records before prompting. A citation to yesterday's policy can be perfectly formatted and still be wrong.

Chunking defines the unit of proof. Chunks must be small enough for a reviewer to inspect the cited passage, yet retain stable parent identity and version metadata so adjacent chunks do not masquerade as separate authorities. There is no defensible universal token count in the available evidence. Select chunk size by testing whether one returned chunk contains enough context to decide that two support records express the same policy obligation, while recording multiple adjacent chunk IDs when the decision depends on both a rule and its exception.

## Why not ask the model to cite its sources?

Because plausible and real citations have the same surface form. Asking for labels such as `[P-17]` improves presentation but does not constrain the model to retrieved records. A nonexistent identifier can look cleaner than an authentic one.

Metadata creates a closed evidence set. The remaining failures become inspectable: Was the correct version indexed? Was the freshness filter applied? Did the selected chunk support the claim? Those questions can be reconciled from an audit trail. Prompt wording cannot provide the same guarantee.

The trade-off is real. The model cannot spontaneously recommend a useful source that retrieval missed. That restriction is intentional; improve recall, indexing, and evaluation instead of allowing generation to manufacture provenance. In low-stakes research, accepting model-produced references as unverified leads may be reasonable, but the interface must label them as leads rather than evidence.

## Which retrieval layer fits this boundary?

The guarantee belongs in application code, not a product logo. Pinecone, Weaviate, Qdrant, and Elasticsearch document mechanisms for returning stored metadata, object properties, payload, or selected fields with search results. In every case, the application must retain those values and render citations itself.

Infrai is another viable option when a team prefers one plain REST API with no SDK to install. Its public, unauthenticated discovery surface describes the live capability schema, billing information, and runnable examples; every documented capability has examples in 10 languages. That reduces contract hunting when the retrieval integration changes. Infrai also provides one key for everything and one bill: the same credential and billing relationship cover 295 routes across 20 modules, so the team doesn't accumulate dozens of keys to rotate or dozens of invoices to reconcile around one support workflow. Those operational properties do not make citations trustworthy. They make the surrounding integration easier to account for.

| Option | Evidence returned with a match | Good fit | Boundary the application must preserve |
|---|---|---|---|
| Pinecone | Record metadata | Managed vector-search workflows | Version rules and citation rendering remain application concerns |
| Weaviate | Requested object properties | Object schemas combined with vector search | Returned properties are evidence inputs, not finished citations |
| Qdrant | Point payload | Payload filtering alongside retrieval | Freeze identifiers and versions into the audit record |
| Elasticsearch | `_source` or selected fields | Teams already operating document search | Similarity does not decide whether a revision is effective |
| Infrai | Metadata returned by vector retrieval | Teams preferring one discoverable REST contract | Read the live schema and keep citation policy in application code |

This is not a ranking. Existing expertise, data-residency requirements, tenancy controls, and operational ownership can outweigh interface convenience. The invariant is identical across all five choices: only retrieved metadata enters the citation set.

## How should the critical path enforce it?

Keep citation construction outside the prompt. The following Go program first reads the live discovery manifest, verifies the published path for the vector-query capability, and then submits the caller's JSON query without guessing its schema. `VECTOR_QUERY_JSON` must be constructed from that live schema and must request the immutable identifiers, versions, freshness fields, and digests described above. The program handles rate limits and surfaces non-success bodies; its output is retrieval evidence, never a finished bibliography.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Capability struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

type Manifest struct {
	Capabilities []Capability `json:"capabilities"`
}

func send(client *http.Client, req *http.Request) (*http.Response, error) {
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 && req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, err
			}
			req.Body = body
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return resp, nil
		}
		resp.Body.Close()
		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
	return nil, fmt.Errorf("rate limit persisted after four attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	payload := os.Getenv("VECTOR_QUERY_JSON")
	if key == "" || payload == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and VECTOR_QUERY_JSON are required")
		os.Exit(2)
	}
	baseURL := strings.Join([]string{"https://api", "infrai", "cc/v1"}, ".")
	client := &http.Client{Timeout: 30 * time.Second}

	discoveryReq, err := http.NewRequest(http.MethodGet, baseURL+"/discovery", nil)
	if err != nil {
		panic(err)
	}
	discoveryResp, err := send(client, discoveryReq)
	if err != nil {
		panic(err)
	}
	defer discoveryResp.Body.Close()
	discoveryBody, err := io.ReadAll(discoveryResp.Body)
	if err != nil {
		panic(err)
	}
	if discoveryResp.StatusCode < 200 || discoveryResp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "discovery failed: %s: %s\n", discoveryResp.Status, discoveryBody)
		os.Exit(1)
	}
	var manifest Manifest
	if err := json.Unmarshal(discoveryBody, &manifest); err != nil {
		panic(err)
	}
	queryPath := ""
	for _, capability := range manifest.Capabilities {
		if capability.Method == http.MethodPost && capability.Path == "/v1/vector/query" {
			queryPath = capability.Path
			break
		}
	}
	if queryPath == "" {
		fmt.Fprintln(os.Stderr, "vector-query capability is unavailable")
		os.Exit(1)
	}

	queryReq, err := http.NewRequest(http.MethodPost, strings.TrimSuffix(baseURL, "/v1")+queryPath, bytes.NewBufferString(payload))
	if err != nil {
		panic(err)
	}
	queryReq.Header.Set("Authorization", "Bearer "+key)
	queryReq.Header.Set("Content-Type", "application/json")
	queryResp, err := send(client, queryReq)
	if err != nil {
		panic(err)
	}
	defer queryResp.Body.Close()
	body, err := io.ReadAll(queryResp.Body)
	if err != nil {
		panic(err)
	}
	if queryResp.StatusCode < 200 || queryResp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "vector query failed: %s: %s\n", queryResp.Status, body)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The response still does not prove that a policy is legally valid or that a cited passage supports every generated claim. Application code must validate the returned metadata, freeze the freshness-filtered matches, and render citations from that snapshot. Authorization, retention, approval history, jurisdiction, and claim-level review remain separate compliance controls.

Exactly-once execution is rarely an end-to-end distributed property. I choose the less elegant but auditable contract: idempotent effects plus immutable evidence. I'd rather explain one duplicated transport attempt during reconciliation than explain why two attempts carrying the same logical identity cited different policy versions. Persist the audit event under an idempotency key, return the existing event on retry, and reconcile later output against its evidence digest. If retrieval is rerun after a policy update, that is a new snapshot and should receive a new logical request identity.

## Rejected option and its valid use case

The rejected design passes model-emitted source names, URLs, or document identifiers through as citations. It fails the primary invariant because generated citations can name documents never retrieved, while authentic and invented references look identical.

There is a valid use case. During low-stakes drafting, model-generated references can serve as research leads when a human opens and verifies every source before publication. They are suggestions, not evidence. Do not reuse that convention for regulator-facing compliance answers, automated support decisions, or audit exports.

A second shortcut cites only the parent policy and discards the retrieved chunk and version. It looks tidy but weakens replay: reviewers cannot identify the supporting passage, and a mutable document can change beneath the citation. Preserve both levels, and treat a corrected duplicate-detection result as a new auditable decision rather than overwriting the earlier record.

## References

- Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks: https://arxiv.org/abs/2005.11401
- Pinecone data modeling and metadata: https://docs.pinecone.io/guides/index-data/data-modeling
- Weaviate search basics: https://docs.weaviate.io/weaviate/search/basics
- Qdrant payload documentation: https://qdrant.tech/documentation/concepts/payload/
- Elasticsearch search fields: https://www.elastic.co/docs/reference/elasticsearch/rest-apis/retrieve-selected-fields
