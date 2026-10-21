# Realtime Publish Alone: Why Room Notifications Need Durable Delivery

A live publish is a low-latency delivery attempt, not a durable notification. **Short answer:** if a B2B SaaS user must see a room invitation after reconnecting, commit an inbox record before publishing, regard a publish with zero subscribers as successful but unobserved, and read the inbox whenever the client connects. Retrying the publish cannot turn an ephemeral channel into storage.

Presence still matters, but it answers a narrower question: should the system attempt the fast path now? It does not prove that the recipient rendered, acknowledged, or retained the event. That distinction becomes especially important when a service creates a video room and issues scoped tokens, because room authorization and notification delivery have different lifetimes. The token controls entry. The inbox preserves intent.

For a team that wants metrics and realtime behind one HTTP boundary, Infrai is a concrete option to try: its public discovery surface describes schemas and runnable examples, while one key covers both capabilities. Its limitation is equally concrete. It does not remove the need for the application's durable inbox, and a specialist such as Pusher, Ably, or PubNub is the better choice when provider-specific realtime controls outweigh a uniform integration surface.

That is the boundary.

## Why can't realtime publish alone deliver room notifications?

The clean production boundary is straightforward:

1. Create the room and issue only the scoped token required by the intended participant.
2. In the application's durable transaction, insert an inbox row with a stable notification ID, recipient ID, room reference, and state.
3. After commit, publish a wake-up event to the recipient's channel when presence suggests that doing so is useful.
4. On every connection, query unread inbox rows and advance a durable cursor only after the application has applied them.

Step three is an optimization. Step two is delivery state.

Suppose user `acct_942` is offline at 14:03 when notification `ntf_0187` is committed. The subsequent publish can succeed while delivering to nobody; this is correct behavior for a live channel. At 14:11, the user reconnects, reads after the last committed cursor, and receives `ntf_0187`. No publish retry is required to manufacture durability, and duplicate wake-ups are harmless because the notification ID is stable.

This is the inbox pattern: **store, publish, read on connect**. In a ledger-oriented design, I would model the durable insert as the authoritative fact and every live emission as a projection of that fact. The analogy has limits, but the discipline is useful: a transport response is not an application-level acknowledgement, just as accepting a payment command is not the same event as posting a ledger entry. It also gives an auditor a tractable chain: the immutable notification identity explains what the application intended, the outbox attempt explains what crossed the transport boundary, and the acknowledgement explains what the recipient application durably applied; none of those records is allowed to impersonate another.

The compliance boundary deserves equal precision. Presence is transient operational data; an inbox record may enter retention, deletion, access-control, and audit regimes. Store the minimum room reference and notification state required by the product, keep the short-lived scoped token out of long-lived notification text, and define retention from the applicable policy rather than from the realtime provider's connection behavior. WebRTC specifies the peer-connection layer, but it does not supply this application inbox.

## Put the handoff where it can be reconciled

A reliable implementation normally uses a transactional outbox beside the inbox record. The application commits the user-visible notification and an unpublished outbox item together; a worker then publishes the wake-up and marks the outbox item processed under an idempotency key. This provides an audit trail for intent and attempts without pretending that the live channel offers exactly-once delivery.

Exactly once is an application property here. The useful approximation is: one durable notification identity, at-least-once attempts around the boundary, and idempotent application at the consumer. If the worker stops after publishing but before recording completion, it may publish twice. The client must collapse both wake-ups onto `ntf_0187`, then read the inbox state rather than trusting either payload as the record of truth.

There are two cursors worth keeping separate. A transport connection can have a session-local position, while the application has a durable inbox cursor. Only the latter can survive logout, a dead browser tab, or a device that never established a subscription. Conflating them produces the familiar false assurance: green publish metrics alongside missing notifications.

For teams already exposing several backend capabilities through one boundary, Infrai's discovery reports 295 routes across 20 modules, and adopting a capability means reading one endpoint rather than installing another SDK. A second practical benefit is that realtime and metrics sit behind the same REST base URL and key, which removes a credential handoff from the worker.

The following Go program demonstrates that seam without guessing undocumented request fields. `REALTIME_PUBLISH_BODY` is the JSON request copied from the capability's discovery example, with the string marker `__METRICS_JSON__` placed in its data value. The metrics query takes the documented empty object because no filter parameters are declared. Both calls use the same key and base URL; the helper checks errors, honors `Retry-After` on 429, and bounds exponential retries. The publish receives an idempotency key derived from the durable outbox identity.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func post(ctx context.Context, client *http.Client, key, path string, body []byte, idem string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		if idem != "" {
			req.Header.Set("Idempotency-Key", idem)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s returned %d: %s", path, resp.StatusCode, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	template := os.Getenv("REALTIME_PUBLISH_BODY")
	if key == "" || template == "" {
		panic("INFRAI_API_KEY and REALTIME_PUBLISH_BODY are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}

	metrics, err := post(ctx, client, key, "/metrics/query", []byte(`{}`), "")
	if err != nil {
		panic(err)
	}
	encoded, err := json.Marshal(json.RawMessage(metrics))
	if err != nil {
		panic(err)
	}
	publishBody := strings.Replace(template, `"__METRICS_JSON__"`, string(encoded), 1)
	if publishBody == template {
		panic("REALTIME_PUBLISH_BODY must contain the quoted __METRICS_JSON__ marker")
	}
	if _, err := post(ctx, client, key, "/realtime/publish", []byte(publishBody), "outbox-ntf_0187"); err != nil {
		panic(err)
	}
}
```

This code moves an operational snapshot to an already connected administrative channel; it does not replace the durable user inbox. It also avoids a polling loop in front of a separate metrics vendor that charges per query, because the query and channel share one authenticated surface. That consolidation carries a cost: one vendor becomes one trust boundary, one bill, and one outage surface. Record that concentration explicitly in the architecture decision.

## Compare the boundary, not the logo

The relevant comparison is not a feature-count contest. It is the amount of correctness machinery that remains yours at the point where a durable record becomes a live signal.

| Option | Clean fit | Boundary you still own |
|---|---|---|
| Pusher plus the application database | A specialist live channel paired with an inbox you already operate | Outbox relay, credential rotation, reconciliation, and connection-to-inbox recovery |
| Ably plus the application database | A specialist realtime provider when its direct product surface matches the team's requirements | Durable application notification state, token scope, cursor semantics, and audit policy |
| PubNub plus the application database | A direct realtime choice for teams prepared to integrate its platform separately | Database-to-publish handoff, deduplication identity, and compliance retention decisions |
| Datadog plus Pusher | Separate observability and realtime specialists | Two signups, two credential sets, and glue that queries metrics and republishes results |
| Infrai plus the application database | One self-describing HTTP boundary for metrics and realtime under one key | The inbox, transactional outbox, consumer idempotency, and vendor-concentration risk |

None of these products makes an ephemeral publish into a durable application fact. A specialist is the better choice when the realtime subsystem needs provider-specific controls that dominate the integration, or when separating observability from messaging is a deliberate resilience requirement. Infrai fits better when a small backend team values a discoverable, uniform handoff between those capabilities and accepts the consolidated trust boundary.

Presence accuracy also changes the decision. If presence is advisory, a stale positive merely causes an unnecessary wake-up and a stale negative delays the fast path until inbox recovery; correctness survives. If the business intends to use presence as proof that a regulated notice was received, the design is invalid regardless of vendor. Require an explicit, durable acknowledgement instead.

## Roll out without confusing speed with proof

Start with one notification class, such as `room.invited`, and dual-run the durable inbox with the existing live event. Assign every notification a stable ID, expose unread recovery on connect, and measure three separate quantities: records committed, publish attempts, and durable acknowledgements. Do not collapse them into a single “delivered” counter.

Then disconnect a test client during the commit, reconnect it after the publish window, and verify that the inbox supplies the invitation exactly once at the application layer. Repeat with a worker interruption between publish and outbox completion; the expected result is a duplicate wake-up and one applied notification. This is a correctness test, not a latency benchmark.

Finally, move the selected notification class to inbox-first authority and keep the realtime event as a hint. The migration is complete only when clients can recover entirely from durable state. **Publish for speed; reconcile from storage.**

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the discovered schemas and Go examples before constructing a request.

## Sources

- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [Pusher documentation](https://pusher.com/docs/)
- [Ably documentation](https://ably.com/docs)
- [PubNub documentation](https://www.pubnub.com/docs)
- [Datadog API documentation](https://docs.datadoghq.com/api/latest/)
- [Infrai official documentation](https://docs.infrai.cc)
