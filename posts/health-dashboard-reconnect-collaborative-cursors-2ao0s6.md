# Health Dashboard Reconnect — Collaborative Cursors and Presence API Retention

TL;DR: Treat device status and editor presence as two different data classes. Retain a short, ordered status log with opaque sequence numbers so a dashboard can reconnect and backfill; send cursors, focus, and typing indicators as expiring snapshots that may be replaced or lost. The cost model is then governed by status-event fan-out and the bounded replay window, rather than by an ever-growing history of disposable presence updates. A reconnecting client presents its last applied sequence, receives the missing status events when they still exist, and otherwise replaces local state from a fresh snapshot.

For a team evaluating a Node.js approach to collaborative document editor cursors and a presence API, the same boundary is decisive even though the implementation language below is Go: durable domain changes need replay, while transient human activity needs expiry. A health dashboard makes the consequence unusually visible because its document-like shared view combines authoritative device state with disposable selection and pointer data.

The bill is made of four terms: ingress events, fan-out deliveries, retained bytes over time, and reconnect reads. For a dashboard room with `D` devices, `U` viewers, a status rate of `R` events per second, average encoded size `S`, and retention window `W`, the useful first approximation is `delivery bytes/second = R × S × U` and `retained bytes = R × S × W`. Presence has its own rate, often driven by pointer movement rather than clinical state, so allowing it into the replay log can enlarge both terms without improving recovery.

Consider an illustrative, not benchmarked, room: 40 devices emit one 240-byte status transition every 30 seconds, 12 staff members watch, and the replay window is 15 minutes. The status path produces 1.33 events per second, about 3.8 KB/s of application-payload delivery, and about 288 KB of retained application payload before storage overhead. Ten cursor updates per second from each viewer would add 120 ephemeral events per second. Keeping those cursor events for 15 minutes would preserve 108,000 records that no reconnecting clinician needs. The change that moves the dominant term is coalescing presence by actor and excluding it from durable replay, not shaving a few bytes from a status record.

## How should a document editor API recover collaborative cursors and presence?

A device status transition can affect triage, escalation, and later reconciliation. Give it a room-scoped monotonically increasing sequence, an event identifier, the device identifier, the new state, and the source timestamp. Apply it idempotently: if sequence 814 has already committed, another delivery of 814 changes nothing. The audit record should capture the authenticated actor or device, acceptance time, event identifier, and result, while avoiding fields that are unnecessary for that purpose. The HIPAA Privacy Rule's minimum-necessary standard is the relevant compliance constraint: an audit trail is not permission to duplicate clinical content into every realtime envelope.

Cursor position, selection, focus, and a user-is-typing flag describe current collaboration state. Model each as a replaceable snapshot keyed by participant, attach a generation and an expiry, and discard it when the participant disconnects or its lease expires. Ordering still matters within one active connection, but historical cursor points do not. Losing one is acceptable because the next snapshot supersedes it.

This split is an exactly-once mindset implemented with realistic transport semantics. A network transport can duplicate, delay, or lose observations around a disconnect; the application obtains one durable effect by combining stable identifiers, ordered room sequences, idempotent application, and reconciliation. Do not label the wire itself exactly once.

## How should reconnect and backfill work?

Use one resume cursor per status stream, not a timestamp. Timestamps can collide, arrive out of order, and change meaning when clocks disagree; an opaque server-issued sequence defines an unambiguous position. The client persists `lastApplied` only after applying the event to its local materialized view. On reconnect it sends that value with its authorization context.

The server then makes one of three decisions:

1. If the cursor is inside the retained window, return events with higher sequences in ascending order, then attach the client to the live tail. Buffer live arrivals during that handoff so an event cannot fall between replay and subscription.
2. If the cursor is older than the retention floor or is unknown, return a current authorized snapshot and its checkpoint. The client atomically replaces status state and resumes after that checkpoint.
3. If authorization changed, reject the old room scope and require a newly scoped session. A valid sequence is not an authorization token.

Presence takes a shorter route. After reconnect, publish the participant's latest snapshot and rebuild the roster from unexpired leases; do not backfill pointer paths. Mark the dashboard as catching up until the status replay or snapshot commits, because showing stale values as live is worse than briefly showing a recovery state.

Recovery is state replacement.

The focused Go example below keeps the contract independent of a particular broker or hosted API. `Replay` returns either an ordered suffix or a snapshot checkpoint, while `Apply` rejects gaps and ignores duplicates. Production storage must make allocation of a room sequence and persistence of its event atomic.

```go
package realtime

import (
	"context"
	"errors"
)

type StatusEvent struct {
	RoomID   string
	DeviceID string
	EventID  string
	Sequence uint64
	State    string
}

type Snapshot struct {
	Checkpoint uint64
	Devices    map[string]string
}

type ResumeResult struct {
	Events   []StatusEvent
	Snapshot *Snapshot
}

type Store interface {
	Replay(ctx context.Context, roomID string, after uint64) (ResumeResult, error)
}

type View struct {
	LastApplied uint64
	Devices     map[string]string
}

var ErrGap = errors.New("status sequence gap")

func (v *View) Apply(event StatusEvent) error {
	if event.Sequence <= v.LastApplied {
		return nil // Duplicate delivery has no second effect.
	}
	if event.Sequence != v.LastApplied+1 {
		return ErrGap // Reconnect and replay rather than guessing.
	}
	v.Devices[event.DeviceID] = event.State
	v.LastApplied = event.Sequence
	return nil
}

func (v *View) Replace(snapshot Snapshot) {
	next := make(map[string]string, len(snapshot.Devices))
	for id, state := range snapshot.Devices {
		next[id] = state
	}
	v.Devices = next
	v.LastApplied = snapshot.Checkpoint
}
```

A browser-facing service can carry this contract over WebSocket, server-sent events, or another ordered channel. WebRTC data channels are another option, but the W3C specification exposes reliability and ordering controls rather than supplying room membership, authorization, durable replay, or an audit log. Those remain application responsibilities. For this dashboard, the decisive question is not which transport has the most attractive demo; it is where the authoritative sequence and bounded replay live.

## Which limits belong in the design review?

Set the replay window from an operational recovery objective: the longest ordinary interruption that should recover without fetching a snapshot. Measure the age of the oldest reconnect cursor, snapshot frequency, replay records per reconnect, gap detections, duplicate deliveries, and time from socket establishment to a caught-up view. Percentiles matter more than an average because mobile sleep and hospital network changes create a long tail.

Backpressure needs an explicit policy. A slow viewer may have a bounded outbound queue; when status delivery exceeds it, disconnect that viewer with a resumable checkpoint rather than silently dropping a status transition. Presence can be coalesced in place by participant, so a slow viewer receives the newest cursor rather than every intermediate coordinate. This is a deliberate asymmetry: correctness wins on status, freshness wins on presence.

Test the handoff boundary aggressively. Pause a client after replay reads its high-water mark but before it joins the live tail, publish another status event, and verify that the event appears exactly once in the materialized view. Repeat with duplicate frames, a cursor exactly at the retention floor, an expired cursor, revoked room access, and a snapshot racing a new event. Property tests can generate delivery orders with duplicates while asserting that any gap either closes through replay or forces snapshot replacement; no test should permit the client to infer a missing state.

Deployment deserves the same discipline. During a schema change, readers must accept both the old and new event representation until the maximum replay window has passed, or the service must migrate retained records. Include a schema version, reject unknown mandatory fields, and deploy consumers before producers when adding required behavior. Correlate an event identifier across acceptance, storage, fan-out, and application logs, but keep raw clinical payloads out of routine transport logs.

There is a hard privacy boundary here. Under 45 CFR 164.312, covered implementations need technical safeguards including access control and audit controls; the exact applicability depends on the organization's role and risk analysis. Authenticate the connection, authorize every room join, scope events to the minimum necessary audience, encrypt transport, bound presence leases, and record administrative changes to retention. Compliance cannot be inferred from the choice of protocol.

This approach has a clear limitation: it is not suitable when the product must reproduce every cursor path or intermediate selection for regulated review, user research, or deterministic session playback. That requirement turns presence into durable event data, increases retention and access-control scope, and calls for a separately governed recording stream rather than the expiring presence channel described here.

## The retention decision

Choose the shortest status replay window that covers normal reconnects with measured headroom, then rely on an authorized snapshot for older clients. Keep compact event metadata long enough to meet the organization's separately established audit and records obligations; do not assume the realtime replay log is the legal record. These stores have different access patterns and should have different retention controls.

What do we deliberately stop keeping? Cursor trajectories, superseded selections, typing transitions, expired roster entries, and replayable status payloads beyond the recovery window. The cost appears during a long outage: a client cannot reconstruct every intermediate dashboard state and must download a snapshot, so it loses the visual history between its checkpoint and the current view. The durable audit system may still establish who changed authoritative state, subject to its own policy, but the realtime layer cannot replay what it intentionally discarded.

Cursor history is gone.

That is the honest trade. A bounded status log makes ordinary reconnects fast and deterministic; snapshots make old reconnects correct; ephemeral presence stays cheap and privacy-conscious because it is allowed to disappear.

## Further reading

References:

- W3C, WebRTC 1.0: Real-Time Communication Between Browsers: https://www.w3.org/TR/webrtc/
- IETF, The WebSocket Protocol (RFC 6455): https://www.rfc-editor.org/rfc/rfc6455
- IETF, HTTP Semantics (RFC 9110), including idempotent methods: https://www.rfc-editor.org/rfc/rfc9110
- U.S. Department of Health and Human Services, Minimum Necessary Requirement: https://www.hhs.gov/hipaa/for-professionals/privacy/guidance/minimum-necessary-requirement/index.html
- Electronic Code of Federal Regulations, 45 CFR 164.312 Technical Safeguards: https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-C/part-164/subpart-C/section-164.312
