# Sharded Raft key/value database

## Contents

- [Architecture and execution guide](#architecture-and-execution-guide)
- [Implementation techniques](#implementation-techniques)
- [Run verification](#run-verification)
- [Run a persistent cluster](#run-a-persistent-cluster)
- [HTTP reference](#http-reference)
- [Bounds and limitations](#bounds-and-limitations)

The [hosted native k6 measurement](docs/benchmarks/2026-10-22-k6-v2.md) completed a 100 transactions/s, 120-second run: all 12,001 native starts finished (12,000 requested plus one framework boundary offer), with zero dropped or failed logical transactions. Started transaction-group p50/p95/p99 was 1.934 / 3.509 / 4.687 ms. At 1,024 bytes, 95% reads and eight VUs, five-repeat p99 had median 5.269 ms (range 3.912–7.369). Six one-VU matrix trials retained 11 drops; both migration and leader-loss journeys passed exact readbacks. Four visible EPYC 9V45 cores, Go 1.27.3 and k6 2.3.0; this fixed offered workload does not establish maximum throughput or intended-arrival latency.

The [version-2 driver](docs/benchmarks/k6-v2-plan.md) is reproducible with `python tools/benchmark.py benchmark` on isolated Linux; `smoke` owns the nine-node cluster and checks exact committed values. Source, environment, full medians/ranges, raw hashes, negative cells and resource limits accompany the results. Earlier invalid fixtures and failed raw verdicts remain preserved. Native hot-path CPU profiling and replication-index telemetry remain unmeasured; shard count remains fixed at four.

A bounded teaching database with two data groups and a replicated configuration controller. Each group runs three independent processes. Every node has its own TCP Raft transport, durable Bolt log, stable state, and file snapshots. New code lives in `sharddb` and `cmd/sharded`.

This is an independently implemented, course-inspired project. It contains no MIT lab skeleton, grader, or student solution. It does not claim MIT assignment completion. The design follows the mechanisms described in the [2026 sharded-service requirements](https://pdos.csail.mit.edu/6.824/labs/lab-shard1.html) and the [Raft paper](https://raft.github.io/raft.pdf). Exact MIT lab code remains subject to its [course publication policy](https://pdos.csail.mit.edu/6.824/general.html).

The complete [hraftd source](https://github.com/otoolep/hraftd) and upstream Git history are preserved at revision `97a01648bd9e2dfc4c95c3c25a3621dd8d153adb`. Philip O'Toole's MIT license and original example API remain intact. [README.upstream](README.upstream) is the exact original README. HashiCorp Raft provides consensus rather than a new consensus implementation. Dependencies retain their own licenses, including Raft's [MPL 2.0 license](https://github.com/hashicorp/raft/blob/v1.7.0/LICENSE).

## Run verification

Use Go 1.25 or newer and Python 3.11. The recorded Windows run uses Go 1.27.3. No Python packages, cloud service, container engine, or API key are required.

```powershell
go test ./...
go vet ./...
python -m unittest discover -s tools -p test_*.py
go build -o sharded.exe ./cmd/sharded
python tools/verify.py
```

The verifier starts nine actual processes on distinct loopback ports and stores each node in a separate directory. It prints the retained `evidence/journey-<run>` directory. `result.json` records the verdict. `history.json` records logical operation invocation and return times, inputs, results, client identities, epochs, and groups. `events.json` records owned PIDs, crashes, and assertions. Per-process logs show Raft elections, snapshots, and restore. Cleanup stops only the run's owned processes and retains the evidence and state.

Each journey begins with `passed: false` and finalizes its result only after cleanup. Failed journeys retain their error and process/proxy cleanup status, then preserve their nonzero exit. Run `python tools/verify.py --fail-after-doctor` to exercise the controlled failure path. It exits 1 after the nine nodes are ready and must retain a failed verdict with successful cleanup. CI checks that behavior. Historical evidence folders are never rewritten to add missing verdicts.

The project [verification skill](.agents/skills/verify-sharded-database/SKILL.md) and [feature map](.agents/skills/verify-sharded-database/features/README.md) describe the maintained driver. The baseline command is `go build -o baseline.exe .` followed by `python tools/verify.py --baseline`.

## Run a persistent cluster

Create a local JSON configuration with a random 24 to 128 byte `Token` and nine `Nodes`. Each node needs `ID`, `Group`, `HTTP`, `Raft`, and `Dir`. Groups are exactly `controller`, `a`, and `b`, with three members each. All addresses must be numeric loopback addresses. All node IDs, ports, and storage directories must differ. The verifier's generated `cluster.json` is a concrete working example. Keep this administrative capability file private.

For each node, start a separate terminal with its ID and the same configuration file. For example, to resume a verifier-created controller after the verifier exits:

```powershell
.\sharded.exe -config evidence\journey-<run>\cluster.json -node controller0
```

Start the remaining eight IDs from that file. Restart each node with the same file and directory to recover its durable state. Do not change membership or addresses in an existing cluster. Empty directories bootstrap the fixed three-member groups. The original hraftd executable remains available through `go run .` and its original commands in README.upstream.

Use the retry client from Python. Operations for one client identity must be serialized. Preserve its sequence across reconnects; after an uncertain outcome, retry that exact identity and sequence before issuing another operation.

```python
import json
from tools.client import Client
config = json.load(open('cluster.json'))
client = Client(config, 'my-session')
client.op('put', 'hello', 'world')
client.op('append', 'hello', '!')
assert client.op('get', 'hello') == 'world!'
```

## Architecture and execution guide

This project adds `sharddb` and `cmd/sharded` to the preserved `hraftd` example. HashiCorp Raft provides consensus, TCP transport, Bolt-backed logs, and snapshots. The added state machine supplies four fixed shards, a three-node configuration-controller group, and two three-node data groups.

The boundary map shows the client, controller, data groups, and their independent Raft state.

```mermaid
flowchart LR
    client["Retrying HTTP client"] --> api["Node HTTP API"]
    api --> controller["Controller Raft group"]
    api --> data["Data Raft groups a and b"]
    controller -->|"migration commands"| data
    controller --> disk1["Controller Bolt logs and snapshots"]
    data --> disk2["Per-node Bolt logs and snapshots"]
```

Each process serves one HTTP endpoint and participates in one fixed Raft group. The controller stores shard ownership and migration progress. Data-group state machines apply key operations and migration commands. Client operations go to the group that owns the shard at the committed configuration epoch.

This flow follows a key operation and shows how the controller's persisted migration phases recover after a leader change.

```mermaid
flowchart TD
    subgraph Client["Client and HTTP boundary"]
        request["Assign key to fixed shard"] --> config["Read committed owners"]
        config --> submit["Submit operation with epoch and sequence"]
    end
    subgraph Data["Data Raft group"]
        submit --> leader["Group leader commits FSM operation"]
        leader --> check["Check owner, epoch, and client receipt"]
        check --> result["Apply get, put, or append"]
    end
    subgraph Migration["Controller recovery loop"]
        intent["Persist move intent"] --> freeze["Freeze donor"] --> install["Install receiver state"]
        install --> publish["Commit new owner and epoch"] --> activate["Activate receiver"] --> cleanup["Clean donor tombstone"]
        cleanup --> done["Clear completed move"]
    end
    result -->|"response or retry after timeout"| Client
```

The client retries bounded HTTP failures against known group members while preserving its sequence. A timed-out operation can have committed, so it remains uncertain until the same client identity and sequence is retried. During a move, the donor freezes before the receiver activates. The persisted phase record lets the controller leader resume; the shard can be temporarily unavailable during this handoff.

| Subsystem | Responsibility | Owned state or invariant | Source |
| --- | --- | --- | --- |
| Node lifecycle | Validate fixed membership and open Raft, storage, and HTTP | Nine nodes, three per group, distinct loopback addresses and directories | [`sharddb/node.go`](sharddb/node.go) |
| Data state machine | Apply key operations and migration actions | Four shards; owner epoch, values, per-client receipts, and freeze mode | [`sharddb/state.go`](sharddb/state.go) |
| HTTP service | Authenticate and route operations to the local leader | Bearer capability and bounded request slots | [`http/service.go`](http/service.go) |
| Controller mover | Persist and advance migration phases | One in-flight move and monotonic phase/epoch checks | [`sharddb/node.go`](sharddb/node.go) |
| Retry client | Refresh configuration and retry leaders | Stable client identity and increasing per-operation sequence | [`tools/client.py`](tools/client.py) |

## Implementation techniques

The cards cover the active `cmd/sharded` and `sharddb` paths, their direct HashiCorp Raft integration, and the retry methods in `tools/client.py`. Rationale is inferred from the current code. The original `hraftd` API and retained examples are outside this section.

| Subsystem | Technique cards | Source files |
| --- | --- | --- |
| Fixed key partitioning | [Fixed hash sharding](#technique-fixed-hash-sharding) | [`sharddb/state.go`](sharddb/state.go), [`tools/client.py`](tools/client.py) |
| Committed data operations and epochs | [Committed state-machine operations](#technique-committed-state-machine-operations), [Epoch fencing](#technique-epoch-fencing) | [`sharddb/node.go`](sharddb/node.go), [`sharddb/state.go`](sharddb/state.go) |
| Migration | [Recoverable phased shard migration](#technique-recoverable-phased-shard-migration) | [`sharddb/node.go`](sharddb/node.go), [`sharddb/state.go`](sharddb/state.go) |
| Durability and recovery | [Raft log and snapshot recovery](#technique-raft-log-and-snapshot-recovery) | [`sharddb/node.go`](sharddb/node.go), [`sharddb/state.go`](sharddb/state.go) |
| Client retries | [Sequence receipts and request fingerprints](#technique-sequence-receipts-and-request-fingerprints), [Bounded leader and epoch retry](#technique-bounded-leader-and-epoch-retry) | [`sharddb/state.go`](sharddb/state.go), [`tools/client.py`](tools/client.py) |
| HTTP boundary | [Authenticated bounded request handling](#technique-authenticated-bounded-request-handling) | [`sharddb/node.go`](sharddb/node.go) |

<a id="technique-fixed-hash-sharding"></a>

<details>
<summary>Fixed hash sharding</summary>

**Mechanism.** `KeyShard` applies 32-bit FNV-1a to the UTF-8 bytes of a key, then takes the remainder modulo four. The Python retry client uses the same constants and arithmetic. The resulting shard number selects the committed owner entry and the data-group command target.

**Example.** For a key whose hash remainder is `2`, both the client and FSM route it to shard 2, initially owned by group `a`.

**Why here.** A fixed mapping lets the controller move ownership without changing which shard contains a key.

**Tradeoffs.** Changing the hash or shard count would remap keys and requires a data migration. The mapping does not promise balanced key counts.

**Code and ownership.** [`KeyShard`](sharddb/state.go#L67) is extension code. [`shard`](tools/client.py#L4) duplicates the mapping at the client boundary.

</details>

<a id="technique-committed-state-machine-operations"></a>

<details>
<summary>Committed state-machine operations</summary>

**Mechanism.** The node serializes each command and submits it with `Raft.Apply`; the FSM changes state only when Raft applies the log entry. Get, Put, Append, and controller configuration queries use this route. The mutex protects the FSM state while applying a committed entry and while making a snapshot copy.

**Example.** A Get is encoded as a command and submitted through the group log, so the handler does not return a follower's local map value.

**Why here.** Routing reads through the log gives reads the same group ordering as writes and migration changes.

**Tradeoffs.** Reads need a leader and quorum. This costs availability and log work compared with a local or lease-based read.

**Code and ownership.** [`Node.apply`](sharddb/node.go#L184) and [`FSM.Apply`](sharddb/state.go#L93) are extension code using inherited HashiCorp Raft ordering.

</details>

<a id="technique-epoch-fencing"></a>

<details>
<summary>Epoch fencing</summary>

**Mechanism.** Each shard carries an owner group and monotonically increasing epoch. Data commands must match the local shard epoch and serving mode. Controller begin and phase commands also compare the expected owner, next epoch, shard, and current phase before advancing.

**Example.** After ownership advances from epoch 1 to 2, an operation carrying epoch 1 fails the FSM's owner check even if it reaches the former group.

**Why here.** Epoch checks reject delayed requests and stale migration actions after leadership changes or retries.

**Tradeoffs.** Epochs prevent stale commands from taking effect, but do not keep a shard available while neither copy is serving. This is not a cross-group atomic transaction.

**Code and ownership.** [`FSM.apply`](sharddb/state.go#L102) enforces data and controller checks; [`Node.step`](sharddb/node.go#L282) supplies phase transitions. Extension code.

</details>

<a id="technique-recoverable-phased-shard-migration"></a>

<details>
<summary>Recoverable phased shard migration</summary>

**Mechanism.** The controller commits a move intent, freezes the donor through its Raft group, installs the frozen values and receipts as staged receiver state, publishes the new owner and epoch, activates the receiver, retires the donor, then clears the move record. The phase is durable controller state. Repeating a phase action is accepted where safe, so a new leader can continue after a lost response.

**Example.** At phase 1, the controller asks the donor to freeze and return a copy, then sends that copy to the receiver's install command.

**Why here.** Explicit durable steps allow recovery without a distributed transaction spanning the three Raft groups.

**Tradeoffs.** Freezing before activation can make the shard unavailable during the handoff. Both groups and the controller need quorums to complete it.

**Code and ownership.** [`Node.step`](sharddb/node.go#L282) coordinates phases; [`FSM.apply`](sharddb/state.go#L102) validates and persists transitions. Extension code.

</details>

<a id="technique-raft-log-and-snapshot-recovery"></a>

<details>
<summary>Raft log and snapshot recovery</summary>

**Mechanism.** Each node opens its own Bolt-backed Raft log and stable store plus a file snapshot store retaining two snapshots. Existing state determines whether to bootstrap the fixed group. The FSM snapshots its state as JSON; Raft's snapshot sink persists the bytes, and restore decodes at most 16 MiB into FSM state.

**Example.** On restart, `Open` checks `HasExistingState` before calling `BootstrapCluster`, allowing persisted Raft state to recover rather than re-bootstrap.

**Why here.** Durable logs and snapshots let each independent process recover its replicated state after restart.

**Tradeoffs.** Snapshots serialize the complete FSM state and the restore limit bounds accepted data. HashiCorp Raft and raft-boltdb implement the underlying replication and storage mechanisms; this project configures and supplies the FSM hooks.

**Code and ownership.** [`Open`](sharddb/node.go#L74), [`FSM.Snapshot`](sharddb/state.go#L260), and [`FSM.Restore`](sharddb/state.go#L274). Integration is inherited/dependency; snapshot representation is extension code.

</details>

<a id="technique-sequence-receipts-and-request-fingerprints"></a>

<details>
<summary>Sequence receipts and request fingerprints</summary>

**Mechanism.** For each client on each shard, the FSM retains the latest sequence, a SHA-256 fingerprint of operation, key, and value, and the prior result. A matching sequence and fingerprint returns the saved result without applying the effect again. A lower sequence or same sequence with different inputs is rejected. Migration and snapshots carry the receipt ledger with shard values.

**Example.** If an Append commits but its HTTP reply is lost, retrying the same identity and sequence returns its stored value instead of appending again.

**Why here.** Receipts resolve uncertain client outcomes across leader retries and shard moves.

**Tradeoffs.** The ledger stores only the latest sequence per client per shard and is capped at 64 clients per shard. This does not guarantee exactly-once external side effects.

**Code and ownership.** [`Receipt`](sharddb/state.go#L19), the operation branch in [`FSM.apply`](sharddb/state.go#L102), and [`validPart`](sharddb/state.go#L241). Extension code.

</details>

<a id="technique-bounded-leader-and-epoch-retry"></a>

<details>
<summary>Bounded leader and epoch retry</summary>

**Mechanism.** The Python client scans configured members of a group until a monotonic deadline, retaining the latest transport or response error and pausing briefly between scans. For each operation it refreshes committed controller configuration, attaches the current epoch, and retries through the current owner group with a shorter per-attempt deadline. It preserves the same sequence when the result remains uncertain.

**Example.** A leadership change causes the client to try another configured group member, then fetch ownership again before retrying the operation.

**Why here.** Fixed membership makes a bounded member scan sufficient for this teaching cluster.

**Tradeoffs.** Retries can return a timeout with an indeterminate operation outcome. The caller must reuse the same identity and sequence; the client does not retry indefinitely.

**Code and ownership.** [`Client.group`](tools/client.py#L19) and [`Client.op`](tools/client.py#L33). Extension code.

</details>

<a id="technique-authenticated-bounded-request-handling"></a>

<details>
<summary>Authenticated bounded request handling</summary>

**Mechanism.** The handler compares the bearer capability in constant time, admits at most 16 concurrent requests through a buffered channel, caps request bodies at 4 MiB, rejects unknown JSON fields and trailing data, and requires POST for mutating routes. The HTTP server also sets header, read, write, idle, and header-size limits.

**Example.** A seventeenth simultaneous request receives HTTP 429 without entering route processing.

**Why here.** These checks bound resource use and protect the single-tenant administrative HTTP boundary.

**Tradeoffs.** The capability is shared cluster-wide, and Raft TCP trusts local processes. The boundary is not intended for hostile multi-tenant deployment.

**Code and ownership.** [`Node.handle`](sharddb/node.go#L195) and HTTP server setup in [`Open`](sharddb/node.go#L141). Extension code using Go's HTTP and constant-time comparison facilities.

</details>

Terms used in the operation and handoff paths: [Raft](#concept-raft), [state machine](#concept-state-machine), [shard](#concept-shard), [epoch](#concept-epoch), [fencing](#concept-fencing), [migration](#concept-migration), [idempotency](#concept-idempotency), and [linearizability](#concept-linearizability).

<a id="architecture"></a>

### Follow a key operation

1. Start each node with the same fixed configuration and its own persistent directory. An empty directory bootstraps that node's three-member Raft group.
2. The client maps the key to one of four shards and fetches committed owner information from the controller group.
3. The client sends the operation, epoch, identity, and sequence to a data-group member. A follower returns HTTP 409; the client tries another member.
4. The group leader commits the operation through Raft. Its FSM checks epoch and client receipt before changing values or returning a read.
5. The client records the result. After an uncertain timeout, it retries the same identity and sequence before issuing another operation.
6. For a move, the controller commits intent, freezes the donor, stages receiver state, publishes ownership, activates the receiver, and cleans the donor. A restarted controller resumes the recorded phase.

### Design decisions and limits

These rationales are inferred from the current implementation. They do not record historic design intent.

| Choice | Practical rationale | Alternative and cost | Reconsider when |
| --- | --- | --- | --- |
| Use HashiCorp Raft as the consensus library | The extension needs sharding and recovery behavior while preserving the upstream example | Implementing Raft locally adds another consensus implementation and safety surface | The exercise requires changing consensus internals |
| Keep three fixed groups and four shards | Small topology makes ownership and migration journeys explicit | Dynamic membership scales topology but needs reconfiguration and discovery | The deployment needs elastic groups or shard counts |
| Persist migration as numbered phases | A leader can resume work after restart and each step checks epoch | Atomic cross-group transactions need a coordinator protocol with broader coupling | Cross-shard atomicity becomes a requirement |
| Freeze before receiver activation | Prevents intentional concurrent service from both owners | Overlap improves availability but requires conflict resolution or fencing at every read/write | Temporary unavailability is unacceptable |
| Retain per-client sequence receipts | Retries suppress repeated effects for the latest sequence per shard | Time-based eviction saves space but can repeat old operations | Client counts exceed the explicit 64-ledger bound |

### Glossary

<a id="concept-raft"></a>

<details><summary>What is Raft?</summary>

Raft is a consensus protocol that orders log entries across a group. This project uses HashiCorp's implementation; a key write is applied by the group's committed state machine.
</details>

<a id="concept-state-machine"></a>

<details><summary>What is a state machine?</summary>

A state machine applies ordered commands to state and returns results. Here `sharddb.FSM` owns shard maps and accepts only commands with valid epochs.
</details>

<a id="concept-shard"></a>

<details><summary>What is a shard?</summary>

A shard is a partition of keys assigned to a data group. FNV-1a maps keys into four fixed shards; each shard records its current group and epoch.
</details>

<a id="concept-epoch"></a>

<details><summary>What is an epoch?</summary>

An epoch is the monotonically increasing ownership version for a shard. A request with an old epoch is rejected after a migration publishes a newer owner.
</details>

<a id="concept-fencing"></a>

<details><summary>What is fencing?</summary>

Fencing rejects commands from a stale owner or controller. The FSM checks shard epoch and mode before accepting a data or migration command.
</details>

<a id="concept-migration"></a>

<details><summary>What is shard migration?</summary>

Migration transfers shard values and client receipts between groups while changing the owner epoch. The controller records six phases, from intent through clearing the completed move.
</details>

<a id="concept-idempotency"></a>

<details><summary>What is idempotency?</summary>

Idempotency means retrying the same identified operation does not apply its effect twice. This implementation remembers the last sequence and fingerprint for each client on each shard, not an unlimited history.
</details>

<a id="concept-linearizability"></a>

<details><summary>What is linearizability?</summary>

Linearizability means each completed operation appears to take effect at one point between its call and response. Data operations here commit through their group Raft log; `/status` is diagnostic and is not a linearizable read.
</details>

## Architecture

FNV-1a assigns keys to four fixed shards. Initially, shards 0 and 2 belong to group `a`, and shards 1 and 3 belong to group `b`. Each shard has an epoch, an ownership mode, a key map, and a per-client receipt ledger. Get, Put, and Append all execute as committed Raft FSM operations. A follower's local map is never used for the new public Get. Configuration queries also commit through the controller Raft group.

The controller admits one migration at a time with a compare-and-set on the current owner and next epoch. It persists the intent before contacting either data group. Its leader resumes the following recorded phases after election or restart:

| Stored phase | Next action |
| --- | --- |
| 0, intent | Freeze the donor through its Raft log. |
| 1, frozen | Fetch the frozen donor state and commit a staged receiver install. |
| 2, installed | Publish the receiver and new epoch in the controller log. |
| 3, published | Activate the receiver through its Raft log. |
| 4, activated | Clean the frozen donor, preserving its epoch tombstone. |
| 5, cleaned | Clear the controller's completed migration record. |

Values and client receipts move together. Every data action and controller transition checks its epoch. Repeated actions converge without replaying a write. Delayed cleanup cannot delete a shard that has returned to its former group. The donor freezes before the receiver activates, so migration can temporarily make a shard unavailable but cannot intentionally serve it from both owners. Stale controllers cannot advance a newer migration through an older phase transition. These are sharding and recovery mechanisms layered on existing Raft, not changes to Raft's safety rules.

The client refreshes committed configuration and tries the fixed group members when a leader or epoch changes. Retries are bounded by a logical deadline, plus an in-flight HTTP timeout. A timeout represents an uncertain outcome. The receipt ledger suppresses duplicate effects for the last sequence of each client on each shard. Older sequences are rejected after a later request. There is no exactly-once external side-effect claim.

## HTTP reference

All requests require `Authorization: Bearer <Token>`. The token grants administrative access to this single-tenant cluster. Raft TCP trusts local processes; this is not a hostile multi-tenant deployment.

| Route | Request | Result |
| --- | --- | --- |
| `GET /status` | No body. | Diagnostic ID, group, leader, and Raft state. This is not a linearizable data read. |
| `POST /config` | `{}` to a controller. | Committed `state.Owners` and any durable `state.Move`. |
| `POST /op` | `Op`, `Key`, `Value`, `Shard`, `Epoch`, `Client`, `Seq`. | Committed `value` or `error`. `Op` is `get`, `put`, or `append`. |
| `POST /move` | `Shard` and `Move` with `Shard`, `From`, `To`, `Epoch`, `Phase: 0`. | Durable intent or fencing error. |
| `POST /snapshot` | `{}` to a leader. | Snapshot completion or error. |
| `POST /internal` | Typed migration or controller command. | Authenticated node protocol. Treat this as an administrative interface. |

Followers return HTTP 409. Invalid authentication, JSON, or methods return HTTP errors. Committed FSM rejection returns JSON `error` with HTTP 200. Missing keys read as an empty string. Request identities are scoped to client and shard, and use SHA-256 fingerprints of operation, key, and value to reject conflicting reuse.

## Bounds and limitations

The database supports four shards, two fixed data groups, at most one migration, epochs up to 1,000,000, 64 keys and 64 client ledgers per shard, 256 byte keys, 1,024 byte values, and 64 byte client IDs. Capacity exhaustion is explicit. Ledgers do not expire, because eviction would allow old retries to repeat effects. New HTTP requests are capped at 4 MiB and 16 concurrent handlers per node. Nodes retain two snapshots. Restore reads at most 16 MiB. HTTP headers, bodies, connections, and peer RPCs have finite timeouts. Raft limits its transport pool and log queue. State directories are trusted local administrator input, never remote HTTP input.

No cross-shard transactions, dynamic membership, TLS, Byzantine behavior, production operation, arbitrary network partition scheduler, throughput, or scalability guarantee is claimed. A single lost member is tolerated by each group; loss of a majority makes that group's operations unavailable. A migration needs both data groups and the controller to regain quorums before it can finish. Storage capacity and request identity retention are intentionally finite teaching constraints.

The original example's `/key` reads remain local follower reads, as upstream implemented them. Only `cmd/sharded` offers the committed read behavior described here.

## Evidence and dependency review

The baseline original tests and HTTP write/read passed before extension. Focused FSM tests cover frozen and staged ownership, duplicate suppression across migration and snapshot restore, cleanup retry after commit and snapshot restore, stale cleanup after reacquisition, malformed transfer rejection, ledger bounds, and controller recovery from every recorded phase. The real journey drives both data groups, overlapping writers and readers during migration, an intermediate controller crash, competing intents, stale configuration, a real data-leader kill, a real lost quorum, snapshots, full-cluster restart, and duplicate retry after migration and restart.

An owned authenticated HTTP response barrier also forwards a real cleanup to the donor, captures its successful commit, and withholds that reply from the controller. The driver snapshots the retired donor, proves a committed controller query remains at phase 4, and kills the controller before phase 5 persists. The next controller must repeat the exact cleanup, finish, and admit another migration. Retained donor snapshot evidence survives later snapshot rotation. The product has no fault-injection endpoint or delay.

The independent history checker explores legal per-key register orderings subject to invocation/return precedence. It collapses retries by identity while retaining the first acknowledgement. It rejects unresolved operations instead of dropping them. The journey resolves its uncertain append before checking the completed history. Negative unit cases prove that impossible reads and a retry that masks a prior acknowledgement fail the checker. This is bounded observed-history evidence, not a proof over all possible executions.

The recorded source scan uses `govulncheck v1.9.0`. `golang.org/x/sys` was updated narrowly to v0.44.0 for [GO-2026-5024](https://pkg.go.dev/vuln/GO-2026-5024); the original imported vulnerable symbol was not reachable. The fixed dependency requires Go 1.25. The final scan reports no vulnerabilities. Tests, vet, and the process driver run locally. CI is defined with read-only repository permissions and retained fault evidence; a hosted CI execution has not been observed. The race detector is unverified because this Windows environment has no C compiler. See [implementation evidence](docs/implementation-evidence.md) for the exact local run.
