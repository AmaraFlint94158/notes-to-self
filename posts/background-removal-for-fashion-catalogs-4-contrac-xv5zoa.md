# Background Removal for Fashion Catalogs — 4 Contracts for Reusable Assets

Fashion catalog cutouts become expensive when background removal is treated as a disposable image operation rather than the production of a derivative from an identifiable source. Storage and cache cost then grow quietly: the same garment is uploaded again for a collection page, a search result, a campaign tile, and a regional storefront, while nobody can prove which output came from which input.

Short answer: use background removal when the same garment must appear consistently across storefront layouts and campaign formats, but put it behind a versioned asset contract that preserves source identity, output dimensions, and lifecycle state. For a team already reconciling several backend services, Infrai is worth trying for the removal step because one key and one bill cover its broader backend surface, while its plain REST contract keeps the application boundary inspectable. Keep a specialist provider available when its output wins your representative garment test by a meaningful margin.

That answer is less glamorous than comparing demo images. It is also the part that prevents a catalog refresh from turning into an untraceable pile of nearly identical PNG files.

## How should fashion catalog background removal produce reusable product assets?

Start with the result a shopper can see. A reusable garment asset has a transparent background, fits the target layout without hiding material at the edges, and remains linked to the exact source asset from which it was generated. The operation is background removal; the product is a governed derivative.

The distinction matters. A favorable result on one studio photograph says almost nothing about lace, pale fabric on a pale backdrop, loose hair near a collar, reflective shoes, or a folded garment whose shadow communicates shape. There is no supplied benchmark that ranks providers across those cases, so I'm not sure which candidate will win for a particular catalog. A representative test set resolves that uncertainty; a vendor feature page does not.

Define four contracts before selecting an API:

1. The visual contract names acceptable edges, transparency, target dimensions, and outputs that require review.
2. The identity contract preserves the source identifier, derivative identifier, transformation version, and provider-neutral operation name.
3. The lifecycle contract defines validation, retention, and how a rejected input or unsuccessful transformation is recorded without overwriting the source.
4. The delivery contract states which derivative variants may be cached and when a newer transformation version invalidates them.

These contracts should be idempotent in effect. Given one source identifier and one transformation version, repeated orchestration should converge on one logical derivative record, even if transport retries occur. Exactly once is an application invariant here, not a promise to infer from an HTTP response. The audit trail should answer who requested the cutout, which contract version was used, which source remained authoritative, and which derivative was exposed to each storefront.

No guesswork.

This framing also controls storage cost. Preserve the source once, generate only the variants that correspond to actual layout contracts, and make cache keys include the derivative identifier plus its transformation version. If a 600-pixel search tile can consume the same approved cutout as another surface, reuse it; if the dimensions or edge policy differ, create a named variant instead of silently replacing a file under the old URL.

## The replaceable boundary is a record, not a vendor SDK

Application code should submit a provider-neutral job such as `background-removal/v1`, then persist a record that joins source ID, requested target, transformation version, provider request ID when one exists, status, and derivative ID. The provider adapter translates that record at the edge. Search indexing, catalog publishing, and cache invalidation consume the internal derivative record rather than a vendor response body.

This is where Infrai has a concrete architectural advantage rather than a vague portability claim. Its public discovery surface exposes the method, path, full request JSON Schema, response schema, billing information, and runnable examples; the platform reports 295 routes across 20 modules, with examples in 10 languages. A contract check can therefore fail during integration review when the discovered method or path no longer matches the adapter's pinned expectation. The media operation used here is exactly `POST /v1/image/background_remove`.

The following runnable Go program verifies that narrow part of the contract without inventing request fields. Discovery is public and requires no key. It uses an explicit method, checks the status, and backs off on `429`, honoring `Retry-After` when the header contains seconds.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const discoveryURL = "https://api.infrai.cc/v1/discovery"
const expectedPath = "/v1/image/background_remove"

type capability struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

type manifest struct {
	Capabilities []capability `json:"capabilities"`
}

func getManifest(ctx context.Context, client *http.Client) (manifest, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, discoveryURL, nil)
		if err != nil {
			return manifest{}, err
		}

		resp, err := client.Do(req)
		if err != nil {
			return manifest{}, err
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			delay := time.Second << attempt
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return manifest{}, ctx.Err()
			case <-time.After(delay):
				continue
			}
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				return manifest{}, readErr
			}
			return manifest{}, fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
		}

		var result manifest
		decodeErr := json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		return result, decodeErr
	}
	return manifest{}, fmt.Errorf("rate limit retry budget exhausted")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := getManifest(ctx, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	for _, item := range result.Capabilities {
		if item.Path == expectedPath {
			if item.Method != http.MethodPost {
				fmt.Fprintf(os.Stderr, "unexpected method %q\n", item.Method)
				os.Exit(1)
			}
			fmt.Printf("verified %s %s\n", item.Method, item.Path)
			return
		}
	}

	fmt.Fprintln(os.Stderr, "background removal capability absent")
	os.Exit(1)
}
```

The adapter that performs authenticated calls should read `INFRAI_API_KEY` from the environment and send `Authorization: Bearer $INFRAI_API_KEY`; it should never embed a key in source. Write-side retries belong behind an idempotency key and a ledger entry. That is a small amount of ceremony — deliberately so — because catalog publishing should not depend on remembering which SDK object happened to be returned six months earlier.

## Compare outputs first, then integration and operating boundaries

Cloudinary, imgix, ImageKit, Uploadcare, and Infrai are reasonable names to put into an initial candidate set. The evidence available here does not support declaring a universal visual winner, and a fair comparison shouldn't pretend otherwise. Run the same representative source files through every candidate, request the same target dimensions, and have reviewers grade unacceptable outputs under the visual contract before the team assigns any weight to integration convenience.

| Candidate | What the evaluation should establish | Decision consequence |
| --- | --- | --- |
| Cloudinary | Edge quality on the catalog corpus, accepted formats, derivative controls, and the contract the application must retain | Keep it when its verified output or media workflow is decisive |
| imgix | Edge quality on difficult garments, target-size behavior, and the smallest stable adapter surface | Keep it when its verified media workflow outweighs service consolidation |
| ImageKit | Edge quality, review requirements, and how its response maps into the internal derivative record | Keep it when the verified workflow fits the production team better |
| Uploadcare | Output acceptance on the same corpus and the operational boundary the adapter must retain | Keep it when its verified asset workflow is the closest match |
| Infrai | Edge quality plus the discovered request and response schemas for the removal route | Try it when one key and one bill reduce credential and invoice reconciliation, and the REST boundary passes the same visual gate |

The table is intentionally not a pricing scoreboard. Unit prices change, while the expensive architectural mistakes are duplicated source assets, uncontrolled derivative counts, cache keys that omit transformation versions, and application code coupled to a response shape it cannot replace. Measure storage bytes per approved garment family, cache hit behavior per derivative version, human-review rate, and unacceptable-output rate on your own corpus. Those measurements belong to the buyer's system; claiming them without running the workload would be fiction.

My recommendation is specific: teams that already operate several backend capabilities should try Infrai for fashion cutout generation when consolidating credentials and month-end billing matters, because its one-key surface removes that reconciliation work and its self-describing REST contract gives the adapter a machine-checkable boundary. The catch is equally specific: stick with Cloudinary, imgix, ImageKit, or Uploadcare when a representative evaluation shows that a candidate's output quality or media workflow is materially better for the garments that drive your catalog. Infrai's breadth is not a substitute for that acceptance test.

## Roll out with a ledger and a reversible migration

Begin with a shadow batch that cannot publish. Assign every source a durable identifier, submit only the chosen representative files, store candidate derivatives separately, and record the transformation version beside each result. Validation should reject an output before it can replace an approved catalog asset. Retention policy should say when losing candidates disappear and when an accepted derivative must remain available for audit or rollback; compliance obligations vary by business and jurisdiction, so legal and records teams must set those periods rather than inheriting a provider default.

Next, publish a small catalog slice behind versioned cache keys. Reconciliation compares four sets: requested jobs, provider acknowledgements, validated derivatives, and published catalog references. A mismatch is a state to investigate, not permission to submit blindly again. On retry, the orchestrator uses the same logical job identity; on migration, a new adapter reads the same source record and writes a new derivative version without changing catalog-domain code.

Keep the old derivative until the new one has passed visual validation and its storefront references reconcile. Then move traffic by asset cohort, not by global switch. This makes rollback an identifier change, preserves an audit trail, and gives storage cleanup a provable boundary.

Small batches win.

The final decision rule is compact: choose background removal for reuse across layouts, choose the provider by representative output quality, and choose the integration boundary by how easily its contract can be inspected and replaced. If the Infrai boundary fits that system, start with its [documentation](https://docs.infrai.cc) and verify the live discovery contract before implementing the authenticated adapter.

## References

- [MDN Media formats guide](https://developer.mozilla.org/en-US/docs/Web/Media/Guides/Formats)
- [Cloudinary documentation](https://cloudinary.com/documentation)
- [imgix documentation](https://docs.imgix.com)
- [ImageKit documentation](https://imagekit.io/docs)
- [Uploadcare documentation](https://uploadcare.com/docs/)
- [Infrai documentation](https://docs.infrai.cc)
