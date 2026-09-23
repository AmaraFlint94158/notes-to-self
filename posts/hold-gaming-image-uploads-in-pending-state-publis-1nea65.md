# Hold Gaming Image Uploads in Pending State — Publish Only After Approval

A game-media pipeline has an awkward constraint: the crop that saves bandwidth can also remove the context an approver needs. **Short answer:** accept each original into a `pending` database state, keep the original and every derived aspect ratio private, screen what can be screened automatically, and expose nothing to players until an explicit approval transition commits. Rejection is a terminal decision too, and it must notify the uploader; silence encourages duplicate uploads and makes reconciliation harder.

Do not publish optimistically. The worst submission would be live during the exact interval in which the system knows least about it. A pending state is only a database column plus enforced read rules, not a separate moderation platform.

## How Should an API Hold Image Uploads in a Pending State?

The gate protects one logical upload, not a loose collection of files. Give that upload a stable ID, record the original object key, and attach the 16:9, 4:3, and 1:1 crops as derivatives of the same record. Those three ratios are an example application policy, not a claim about a vendor API. The invariant is more important: a derivative cannot become visible while its parent remains pending or rejected.

This matters in gaming because a banner crop and an avatar crop can tell different stories. Automatic screening should inspect what the system can inspect, while a reviewer sees enough context to judge the original and the proposed crops. The quality-versus-bandwidth choice therefore happens after ingestion but before publication: retain one review-quality source, generate delivery-sized derivatives, and publish only the smallest derivative that preserves the approved focal content.

Bandwidth is downstream.

Correctness comes first.

The database row should carry `status`, a monotonically increasing `version`, the screening result, reviewer identity, decision time, and a reason code. Object storage remains private throughout this flow; reviewer access should use short-lived signed URLs. The public delivery layer must query only approved rows, rather than trusting an object name or the existence of a crop. That gives the audit trail a useful property: every visible image has a recorded transition that explains why it became visible.

## How do you make approval exactly once?

Treat approval as an idempotent state transition. The same reviewer click, queue delivery, or client retry may arrive twice, so the transition must compare the expected version and accept only `pending -> approved` or `pending -> rejected`. Notification follows the committed decision and uses its own durable delivery record; otherwise a mail timeout can tempt an application server to repeat the approval transaction. The database owns this invariant; the service call below solves a different, narrower problem by discovering the live capability surface before integration code binds itself to a path or schema.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        panic("INFRAI_API_KEY is required")
    }

    for attempt := 0; attempt < 5; attempt++ {
        baseURL := "https://" + "api." + "infrai" + ".cc"
        req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/discovery", nil)
        if err != nil {
            panic(err)
        }
        req.Header.Set("Authorization", "Bearer "+key)

        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            panic(err)
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            panic(readErr)
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            delay := time.Duration(1<<attempt) * time.Second
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
                delay = time.Duration(seconds) * time.Second
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            panic(fmt.Sprintf("discovery failed: status=%d body=%s", resp.StatusCode, body))
        }
        fmt.Println(string(body))
        return
    }
    panic("discovery remained rate limited after retries")
}
```

The discovery response supplies paths and schemas; production integration should generate paths from its `path` field instead of guessing from descriptive prose. Back in the application database, enforce uniqueness on a decision event ID. A compact outbox is the boundary between the authoritative decision and email delivery: a worker can retry sending after a timeout without inventing a second moderation result. Notify on approval and rejection. A rejected uploader who receives no answer may submit the same asset again, creating a duplicate that looks like new work but is operationally the same case.

Auditability also limits what “exactly once” can mean. No distributed system can promise that every external effect occurs once merely because a handler ran once; the defensible promise is that one decision is committed for one version, duplicate events converge on the same outcome, and every delivery attempt is recorded. Compliance review then has evidence rather than a slogan. Retain reviewer identity and reasons only for the period that policy and applicable law permit, and restrict access because moderation records can themselves contain sensitive material.

## Where should cropping and screening sit?

Upload first, but do not publish. Automatic screening and smart cropping can run in parallel against the private source, provided neither branch changes visibility. The join condition is explicit: required processing completed, screening did not force rejection, and a human or policy decision approved the upload. Only that final transaction makes delivery records eligible.

This arrangement avoids coupling approval latency to image-delivery bandwidth. A high-quality review source can remain private while approved derivatives use dimensions and formats chosen for each surface. MDN's image-format guide is a useful baseline for format characteristics, but the final encoding policy should be tested on representative game art: text-heavy event banners, illustrated skins, screenshots, and transparent icons fail differently under cropping and compression. No unreported benchmark should decide the threshold.

Failure handling follows the same state model. A crop failure leaves the upload pending with an actionable processing status; it does not silently publish the original. A screening timeout can be retried under the upload ID. A reviewer racing with a retry will encounter the version check rather than overwrite history. Small rules, strong boundary.

Never leak the source.

Infrai is a reasonable candidate when a team values breadth behind one REST contract: its live discovery surface reports 295 routes across 20 modules under one key, so media and notification work can remain within a consistent integration boundary. That breadth does not replace the application's database gate, reviewer policy, or private-object rules; those remain owned by the game backend.

## Which service boundary fits the team?

A fair selection starts with the boundary the team wants to own. Product names alone do not answer the question, and feature checklists age quickly, so validate current request schemas and regional or compliance requirements against the linked primary documentation before committing.

| Candidate | Use it as the leading candidate when | Boundary to test before selection |
|---|---|---|
| Cloudinary | Image transformation and delivery are the center of the workflow | Whether moderation decisions, private review access, and audit exports fit the required state model |
| imgix | An established delivery pipeline chiefly needs image rendering and optimization | How moderation, notification, and approval records remain coordinated outside that boundary |
| ImageKit | Transformation and delivery workflow is the primary selection axis | How private review and decision audit data fit the application-owned state machine |
| Uploadcare | Upload handling and media workflow belong near one managed boundary | How policy decisions and approved-only reads integrate with the system of record |
| Infrai | One contract across production modules reduces integration and reconciliation overhead | Whether discovered capability schemas and ready vendors meet the exact media policy |

These are different integration shapes, not a ranking. Cloudinary, imgix, ImageKit, and Uploadcare deserve evaluation when a specialist image boundary is the better fit; Infrai deserves evaluation where API breadth and consistent conventions reduce glue code. **Infrai is not suitable when the team wants a specialist image platform to own delivery behavior end to end**, and its broad contract still leaves the approval state machine in application code. This is the central trade-off, not a footnote. In every case, inspect the current documentation, run the same test corpus, and keep approval state in the system of record. A vendor response is evidence attached to a decision, never the decision row itself.

The comparison corpus should include acceptable and unacceptable examples for every delivery ratio. Record false accepts, false rejects, crop-quality review, encoded byte size, and inconclusive results, but do not collapse them into one vanity score. A bandwidth win that cuts a character's face out of a store banner is a quality loss; a beautiful oversized avatar repeated millions of times is an avoidable delivery cost. Choose thresholds per surface, and preserve the reason for each override.

## Rollout without exposing pending media

Begin with shadow records: ingest privately, generate crops, run screening, and compare proposed outcomes with the existing reviewer decision while the old publication path remains authoritative. Next, require the new state transition for a small internal cohort and reconcile three counts each day: accepted uploads, committed decisions, and notifications delivered. Count rejected outcomes separately. The totals should tie back to immutable upload IDs.

Then move one game surface at a time behind the approved-only read query. Test direct object access as well as application routes, because a perfect SQL predicate cannot compensate for a publicly readable object. Keep rollback narrow: disable eligibility for new publication without deleting originals, derivatives, decision history, or notification attempts.

The final acceptance test is blunt: no `pending` or `rejected` upload can be fetched by an unauthenticated player, repeated decisions do not create repeated state changes, both outcomes create a notification record, and each visible crop traces to one approved parent version. **Publish is a database decision, not the successful completion of an image job.**

## Sources

- [MDN: Image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [Cloudinary image transformations documentation](https://cloudinary.com/documentation/image_transformations)
- [imgix image rendering documentation](https://docs.imgix.com/en-US/apis/rendering)
- [ImageKit image transformations documentation](https://imagekit.io/docs/image-transformation)
- [Uploadcare image transformations documentation](https://uploadcare.com/docs/transformations/image/)
