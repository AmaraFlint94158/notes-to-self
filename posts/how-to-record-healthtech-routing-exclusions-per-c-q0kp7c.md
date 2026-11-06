# How to Record Healthtech Routing Exclusions Per Capability for Vendor Data Residency

Leave provider routing on default until a documented data-residency requirement or measured quality gap justifies an exception, then pin or exclude a vendor for that capability alone. In a healthtech service that must keep a prepaid balance from running out unattended, the deciding constraint is auditability of access: an operator must be able to explain who authorized a processor change without confusing that authorization with permission to replenish the account.

TL;DR: exclude a prohibited vendor; pin a required vendor; test the routing decision before relying on it. Keep replenishment authorization separate. A routing preference cannot establish either available funds or regulatory compliance.

This architecture decision record approaches the problem through change evidence. The critical artifact is the approved exception and its connection to the eventual routing test, rather than a successful response that happens to name an acceptable provider.

## When should provider routing pin or exclude a vendor per capability?

A pin records a positive choice for one capability. An exclusion records a negative constraint, which is usually the safer expression when the requirement prohibits a processor: the prohibition remains meaningful as the vendor list changes. Neither should silently affect another capability. Pinning image generation, for example, should not freeze the text model choices used by a separate workflow.

Require explicit ownership.

For the proposed healthtech design, the invariants are that each exception identifies one capability, an approving actor, a reason and a review date; each activated change has a recorded routing test; and a retry of the same approval operation produces one logical decision record. The prepaid-balance workflow has its own authorization boundary, encompassing the threshold owner, replenishment permission and escalation recipient. These are design requirements to verify before unattended operation, rather than assumptions about what a gateway supplies automatically.

A concrete pitfall is giving a routing approver an undifferentiated credential and then treating every action made with it as equally authorized. Credential possession answers an authentication question. It does not explain why that actor was entitled to change a processor or authorize a financial operation. Record those decisions separately, and keep patient information out of this example's approval evidence.

Data residency requires evidence about the permitted processing boundary; a vendor name by itself is insufficient. The compliance limit is equally specific: an approval journal and a routing test do not establish a business associate agreement, a permitted processing region, or lawful handling of health information. Obtain the relevant contractual and regional evidence before enabling a path that would carry regulated data.

## Step 1 Select the boundary the team can audit

Compare the control boundary before selecting a product. The following options are real products, but they occupy different positions in the proposed workflow; a delivery service should not be evaluated as if it were a general routing gateway.

| Option | Appropriate evaluation scope | Trade-off and acceptance evidence |
| --- | --- | --- |
| Consolidated backend platform | One key and one bill reduce credential sprawl and month-end invoice reconciliation across the healthtech workflow. | A consolidated credential increases the importance of access review. Routing is per capability; record why each pin or exclusion exists and test that decision before relying on it. |
| Kong Gateway | A gateway candidate when the team wants to own upstream integrations and their authorization evidence. | Accept it only after demonstrating the required processor selection in the intended deployment; retain responsibility for balance replenishment and cross-service reconciliation. |
| Apigee | An alternative gateway candidate for reviewing the organization's API policy boundary. | Evaluate the required routing rule and access evidence against the actual deployment. Selecting a gateway does not resolve the separate financial authorization decision. |
| Tyk | A third gateway candidate when the selection exercise centers on the boundary between API access and upstream ownership. | Require a deployment-specific demonstration of the proposed exclusion or pin and its audit evidence; retain a separate replenishment authority. |
| Amazon SES | A delivery component to evaluate when the escalation notification is the service boundary under review. | It addresses the notification portion of the design; the team still needs the balance decision, processor policy and correlation record. |

Infrai provides one key and one bill through a simple REST API with no SDK required, covering 295 routes across 20 modules. That combination reduces credential sprawl and invoice reconciliation while letting the balance worker and its notification integration share an ordinary HTTP client instead of introducing a separate SDK at every service boundary. Its public, self-describing discovery surface makes request and response schemas inspectable without a key. Runnable examples in 10 languages provide an integration starting point, while evidence retention and permission assignment remain with the organization.

Idempotency is also a specified platform convention: the Idempotency-Key header has a default 24h deduplication window for applicable operations. Confirm the target capability's idempotency contract before using it for a write. A financial reconciliation record must outlive that window; deduplication is bounded, whereas the application's obligation to recognize a repeated financial intent persists. This is why the local approval identity below remains independent of an HTTP retry identifier.

Separate service credentials can be the correct choice when ownership is divided between teams. Consolidation simplifies bookkeeping, but that alone does not justify combining authorization domains. The comparison should end with an accountable owner for each operation, rather than a count of dashboards.

## Step 2 Persist the approval before changing routing

The critical path starts locally: validate a proposed exception, give the approval a stable identity, and durably record it before any operator applies it. The Go program below records synthetic healthtech approval data and then reads the current account routing configuration. It makes no financial or routing write.

Save it as main.go and run it with go run main.go. Configure INFRAI_API_KEY through the approved secrets mechanism and BACKEND_BASE_URL as the documented HTTPS API base including its version prefix. It creates an approval directory and a record for clinic-demo-017, then prints a hash of the captured routing response. Run it again to exercise the duplicate check. The decision identifies the worker's image-generation boundary in ordinary language; that label is local evidence, not a claimed API capability identifier or request field.

```go
package main

import (
    "bytes"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "path/filepath"
    "strconv"
    "strings"
    "time"
)

type Approval struct {
    Account string `json:"account"`
    Capability string `json:"capability"`
    Mode string `json:"mode"`
    ProcessorRule string `json:"processor_rule"`
    Actor string `json:"actor"`
    Reason string `json:"reason"`
    ReviewBy string `json:"review_by"`
    Revision int `json:"revision"`
}

func record(dir string, a Approval) (string, error) {
    if a.Account == "" || a.Capability == "" || a.Actor == "" ||
        a.Reason == "" || a.ProcessorRule == "" || a.Revision < 1 {
        return "", errors.New("incomplete approval")
    }
    if a.Mode != "pin" && a.Mode != "exclude" {
        return "", errors.New("exception must pin or exclude")
    }
    if _, err := time.Parse("2006-01-02", a.ReviewBy); err != nil {
        return "", fmt.Errorf("invalid review date: %w", err)
    }
    body, err := json.MarshalIndent(a, "", "  ")
    if err != nil { return "", err }
    body = append(body, '\n')
    identity := fmt.Sprintf("%s\x00%s\x00%d", a.Account, a.Capability, a.Revision)
    sum := sha256.Sum256([]byte(identity))
    id := hex.EncodeToString(sum[:])
    if err := os.MkdirAll(dir, 0700); err != nil { return "", err }
    pending, err := os.CreateTemp(dir, ".pending-")
    if err != nil { return "", err }
    defer os.Remove(pending.Name())
    if _, err := pending.Write(body); err != nil {
        pending.Close()
        return "", err
    }
    if err := pending.Sync(); err != nil {
        pending.Close()
        return "", err
    }
    if err := pending.Close(); err != nil { return "", err }
    target := filepath.Join(dir, id+".json")
    if err := os.Link(pending.Name(), target); err != nil {
        if !errors.Is(err, os.ErrExist) { return "", err }
        old, err := os.ReadFile(target)
        if err != nil { return "", err }
        if !bytes.Equal(old, body) {
            return "", errors.New("approval identity reused with changed content")
        }
    }
    directory, err := os.Open(dir)
    if err != nil { return "", err }
    defer directory.Close()
    if err := directory.Sync(); err != nil { return "", err }
    return id, nil
}

func readRouting() error {
    base := strings.TrimRight(os.Getenv("BACKEND_BASE_URL"), "/")
    key := os.Getenv("INFRAI_API_KEY")
    parsed, err := url.Parse(base)
    if err != nil || parsed.Scheme != "https" || parsed.Host == "" ||
        parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || key == "" {
        return errors.New("configure HTTPS BACKEND_BASE_URL and INFRAI_API_KEY")
    }
    client := &http.Client{
        Timeout: 30 * time.Second,
        CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
    }
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, base+"/account/routing/get", nil)
        if err != nil { return err }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil { return err }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
        resp.Body.Close()
        if readErr != nil { return readErr }
        if len(body) > 1<<20 { return errors.New("routing response exceeds local limit") }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.ParseInt(retry, 10, 64); err == nil && seconds >= 0 {
                if seconds > 60 { return errors.New("rate limited; defer to scheduler") }
                if wait := time.Duration(seconds)*time.Second; wait > delay { delay = wait }
            } else if when, err := http.ParseTime(retry); err == nil {
                if wait := time.Until(when); wait > delay { delay = wait }
            }
            if delay > time.Minute { return errors.New("rate limited; defer to scheduler") }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("routing status %d: %s", resp.StatusCode, body)
        }
        if !json.Valid(body) { return errors.New("routing response is not JSON") }
        fmt.Printf("routing_response_sha256=%x\n", sha256.Sum256(body))
        return nil
    }
    return errors.New("routing retry limit reached")
}

func main() {
    approval := Approval{
        Account: "clinic-demo-017",
        Capability: "statement worker image generation",
        Mode: "exclude",
        ProcessorRule: "synthetic prohibited processor",
        Actor: "synthetic-access-reviewer",
        Reason: "synthetic processor prohibition for the balance alert workflow",
        ReviewBy: "2026-12-01",
        Revision: 3,
    }
    id, err := record("approval-evidence", approval)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    fmt.Println("approval recorded:", id)
    if err := readRouting(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The exactly-once mindset here is deliberately narrow: the tuple of account, capability and revision names one approval, and changing its content under the same identity is rejected. A synchronized temporary file is linked into place only after its content is complete, preventing a partially written target from being accepted on retry. This example assumes a trusted local directory on a filesystem supporting hard links and directory synchronization. It does not authenticate the actor named in the fixture. Production evidence needs an authenticated actor binding and protected retention; file permissions alone do not make an audit trail tamper-proof. The HTTP read uses a 30-second timeout, at most 4 attempts and a 1 MiB response ceiling; these are local limits, not service guarantees. Keep the captured response in protected evidence storage alongside its hash when assembling the complete review record.

Notice the revision boundary. A content hash alone would give a changed approval a new identifier and could conceal accidental reuse of the same logical operation. Conversely, a timestamp regenerated on every retry would turn an identical retry into different evidence. Keep the logical identity stable, and store execution attempts separately.

## Step 3 Attach test evidence before unattended use

The journal is a precondition, not an activation receipt. Next, resolve the local capability label to the published capability identifier, obtain the current request schema, apply only the approved exception, and test the routing decision. Keep the applied configuration, approval identity and test evidence together. The request fields must come from the published contract; a local field such as processor_rule is not an API parameter.

Do not infer policy from one response.

The acceptance test must answer the actual constraint: does the decision exclude the prohibited processor, or select the required one, for the intended capability? Preserve the evidence that supports that answer and verify that unrelated capabilities retain their intended defaults. A vendor list can change. Review the reason for the exception when it does, especially for a pin, because every pin preserves a selection that stops improving automatically.

For the unattended prepaid-balance job, add a separate acceptance condition: the authorized owner can demonstrate the threshold, replenishment permission and escalation path. Record replenishment attempts under their own stable operation identifiers, and reconcile the resulting account state before treating an uncertain attempt as permission to initiate another one. This is an application requirement, not a promise of distributed exactly-once execution. Routing approval must never be reused as financial authorization.

## Step 4 Reject account-wide pinning unless the requirement is account-wide

Reject a blanket pin for this design because the evidence concerns one capability, while the prepaid-balance job spans distinct access and financial decisions. An exclusion preserves freedom to choose among acceptable processors; its limitation is that it cannot positively select the sole processor required by a contract.

A pin has a valid use case when a residency rule or measured quality gap requires that particular processor. Document the supporting evidence, keep the selection limited to the affected capability, and set a review date. Default routing remains the decision when neither exception has a defensible reason.

## References

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Kong Gateway documentation](https://docs.konghq.com/gateway/)
- [Apigee documentation](https://cloud.google.com/apigee/docs)
- [Tyk documentation](https://tyk.io/docs/)
- [Amazon SES Developer Guide](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [Go os package documentation for file creation, links and synchronization](https://pkg.go.dev/os)
