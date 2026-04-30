# Analysis Workflow

The analysis workflow uses a **captain/worker/verifier** architecture to perform long-running, iterative investigations with built-in verification.

## Architecture Overview

```mermaid
graph TB
    subgraph Engine["Juvenal Engine (deterministic loop)"]
        direction TB
        Schedule["Scheduler<br/><i>dispatch targets to workers,<br/>claims to verifiers</i>"]
        Drain["Future Drainer<br/><i>collect completed results</i>"]
        State["State Persistence<br/><i>.juvenal-state-*-analysis.json</i>"]
    end

    subgraph Captain["Captain (Claude Code)"]
        direction TB
        Mental["Mental Model<br/><i>tracks attack surface,<br/>verified findings, open questions</i>"]
        Targets["Target Proposals<br/><i>bounded analysis tasks<br/>with scope + priority</i>"]
    end

    subgraph Workers["Workers (parallel)"]
        direction TB
        W1["Worker 1<br/><i>investigate target</i>"]
        W2["Worker 2<br/><i>investigate target</i>"]
        W3["Worker N<br/><i>investigate target</i>"]
    end

    subgraph Verifiers["Verifiers (parallel)"]
        direction TB
        V1["Verifier 1<br/><i>check claim</i>"]
        V2["Verifier 2<br/><i>check claim</i>"]
        V3["Verifier N<br/><i>check claim</i>"]
    end

    Captain -->|"target proposals"| Engine
    Engine -->|"dispatch targets"| Workers
    Workers -->|"claims (findings)"| Engine
    Engine -->|"claims to verify"| Verifiers
    Verifiers -->|"verified / rejected"| Engine
    Engine -->|"delta: verified findings,<br/>rejections, exhausted targets"| Captain

    style Captain fill:#4a9eff,color:#fff
    style Workers fill:#ff9f43,color:#fff
    style Verifiers fill:#ee5a24,color:#fff
    style Engine fill:#2d3436,color:#fff
```

## Data Flow

```mermaid
sequenceDiagram
    participant U as User (optional)
    participant C as Captain
    participant E as Engine
    participant W as Worker
    participant V as Verifier

    Note over C: Build mental model<br/>of attack surface

    C->>E: Propose targets<br/>(priority, scope, instructions)
    
    par Worker execution
        E->>W: Dispatch target
        W->>W: Investigate code,<br/>run commands,<br/>gather evidence
        W->>E: Claims with PoC<br/>(or no_findings / blocked)
    end

    par Verification
        E->>V: Claim packet<br/>(scrubbed, independent)
        V->>V: Re-read code,<br/>check guards,<br/>verify PoC
        alt Claim is real
            V->>E: VERIFIED + summary
            E->>C: Delta: claim verified
        else Claim is wrong
            V->>E: REJECTED + rejection_class<br/>+ follow_up_action/strategy
            Note over E: Claim-scoped retry<br/>(if budget remains)
            E->>W: Retry with verifier's<br/>challenge + rejection chain
            W->>E: Updated claim<br/>(new evidence)
            E->>V: Re-verify
        end
    end

    C->>C: Update mental model<br/>with verified findings
    C->>E: New targets based<br/>on evidence

    opt User interaction
        U->>C: /focus, /ignore,<br/>free-form guidance
    end

    Note over C,V: Loop continues until<br/>attack surface exhausted
```

## Claim-Scoped Retry (Verifier Dialog)

When a verifier rejects a claim, only that specific claim is retried — verified sibling claims are preserved.

```mermaid
flowchart LR
    subgraph Target["Target: input-parser"]
        C1["Claim 1<br/>heap-overflow"]
        C2["Claim 2<br/>off-by-one"]
        C3["Claim 3<br/>null-deref"]
    end

    V1[/"Verifier"/]
    V2[/"Verifier"/]
    V3[/"Verifier"/]

    C1 --> V1
    C2 --> V2
    C3 --> V3

    V1 -->|"VERIFIED"| R1["✓ Preserved"]
    V2 -->|"REJECTED<br/>guard-found"| Retry["Retry Worker<br/><i>only claim 2</i>"]
    V3 -->|"VERIFIED"| R3["✓ Preserved"]

    Retry -->|"new evidence"| C2b["Claim 2b<br/><i>retry_count=1</i>"]
    C2b --> V2b[/"Verifier"/]
    V2b -->|"VERIFIED"| R2["✓ All claims resolved"]

    style R1 fill:#27ae60,color:#fff
    style R3 fill:#27ae60,color:#fff
    style R2 fill:#27ae60,color:#fff
    style Retry fill:#f39c12,color:#fff
```

## Interactive Mode (`--interactive`)

With `--interactive`, the runner opens a Rich Live chat dashboard. The
captain still runs as the same `claude --session-id=<uuid>` session it
uses in batch mode (resumed via `claude --resume <uuid>` per turn), but
output is rendered in the dashboard's captain panel and the user can
inject directives at any moment via the chat input.

```mermaid
graph LR
    subgraph DASH["Rich Live Dashboard"]
        CP["Captain panel<br/>(message + mental model)"]
        ES["Event stream<br/>(workers / verifiers / claims)"]
        CI["Chat input<br/>(>>> )"]
    end

    subgraph BG["Runner Loop"]
        CT["Captain turn<br/>(background thread)"]
        WK["Workers"]
        VF["Verifiers"]
        UC["UserInteractionChannel<br/>(stdin reader)"]
    end

    User["User"] -->|"types directive"| UC
    UC -->|"poll(0.0)"| BG
    BG -->|"render hooks"| DASH
    CT -->|"CAPTAIN_JSON"| BG
    BG --> WK
    WK --> VF

    style DASH fill:#4a9eff,color:#fff
    style BG fill:#2d3436,color:#fff
    style User fill:#636e72,color:#fff
```

Directives the user can type at any moment:
`/focus <text>`, `/ignore path:<prefix>`, `/ignore symbol:<name>`,
`/target <text>`, `/ask <question>`, `/now` (force the next captain
turn now), `/show captain` (print the full captain mental model
out-of-band), `/summary`, `/stop`, `/wrap`, or any free-form note.

## Error Handling

```mermaid
flowchart TD
    WE["Worker/Verifier Error<br/>(crash, malformed output)"]
    WE --> Inc["Increment target<br/>error_retry_count"]
    Inc --> Check{"Under<br/>max_retries?"}
    Check -->|"Yes"| Requeue["Requeue target"]
    Check -->|"No"| Block["Mark target BLOCKED<br/>(other targets continue)"]

    CE["Consecutive Errors<br/>(all workers failing)"]
    CE --> Thresh{"Over<br/>threshold?"}
    Thresh -->|"Yes"| Pause["PAUSE analysis<br/>save state for --resume"]
    Thresh -->|"No"| Continue["Continue"]

    Success["Any success"] --> Reset["Reset consecutive<br/>error counter"]

    style Pause fill:#e74c3c,color:#fff
    style Block fill:#e67e22,color:#fff
    style Requeue fill:#f39c12,color:#fff
    style Continue fill:#27ae60,color:#fff
    style Reset fill:#27ae60,color:#fff
```
