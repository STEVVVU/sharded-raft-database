# Version 2: bounded native HTTP workload

Status: main reviewed the adapter and approved integration. The existing launcher and workflow now use k6 v2.3.0 for version 2; real-node k6 smoke and measurements remain unexecuted. Only syntax and pure contract checks ran locally. The version-1 source/history and failed raw artifact remain preserved.

Hosted run [39730696115](https://github.com/STEVVVU/sharded-raft-database/actions/runs/39730696115) failed during its first full fixture setup. Its four-key smoke passed. Version 1 proposed 1,024 keys, 4,096-byte values and growing client identities, exceeding the native limits in `sharddb/state.go`: 64 keys and 64 clients per shard, and 1,024 bytes per value. Preserve that failure as an invalid workload, not a throughput result. No product limit or safety rule changes are proposed.

`tools/benchmark-k6.js` calls the existing authenticated loopback HTTP API directly. Configuration reads and data reads/writes remain actual committed Raft operations across the nine real nodes. No HTTP bridge, gevent compatibility layer, new arrival scheduler or new percentile implementation is needed.

| Version-2 input | Frozen value |
| --- | --- |
| Read fixture | `read-0` through `read-127`, immutable |
| Write fixture | `write-0` through `write-31`, one key per VU |
| Routing | Native FNV-1a, four fixed shards |
| Actual keys per shard | 39 / 41 / 41 / 39, leaving 25 / 23 / 23 / 25 spare |
| Identities | One serialized `seed-v2`, at most 32 serialized `bench-v2-*`, and one final `verify-v2` identity |
| Maximum clients per shard | 34, leaving at least 30 spare |
| Value sizes | 64 and 1,024 ASCII bytes |
| Read transaction percentages | 100 / 95 / 50 / 0, deterministic seed 421 |
| Allocated VU limit | 1 / 8 / 32; each reused identity has one request in flight |
| Offered transaction rates | 5 / 25 / 100 per second, ten-second sweeps |
| Stability | Highest qualifying offered rate for 120 seconds |
| Comparison repetition | Five independent fresh-cluster runs per cell, reversed order between repetitions |

The comparison grid is value size × read percentage × VU limit at 25 offered transactions/s. This holds offered load fixed while changing the client bound. The baseline is one VU with identical data and mix. The rate sweep and sustained run use 64-byte values, 95% reads and 32 VUs. A qualifying rate needs zero dropped or interrupted iterations, zero failed/rejected/uncertain transactions and a transaction p99 below five seconds. These are proposed acceptance criteria pending main review, not results.

A read transaction checks an immutable fixture value. A write transaction performs PUT followed by GET through the same serialized identity and checks both exact values. Write values use index `128 + iteration`, so even the first measured write differs from its seeded value. Its reported latency includes both committed operations, configuration lookups, routing, retries and the readback oracle. Report **transactions/s**, distinguishing `get` and `put_get`; do not label this as single-request PUT throughput. Per-VU write keys prevent another writer from invalidating immediate readback. This is a disjoint-key workload, not a contention or general linearizability proof. Existing real-process history and migration verification remains authoritative for its documented scope.

k6's constant-arrival-rate executor owns scheduling and its `dropped_iterations` metric counts offers it could not start. `transaction_started`, `transaction_finished`, the three explicit error counters, k6 interrupted iterations and raw safe invoke/return receipts must all be retained. Native HTTP 409 leader responses and 429 busy responses remain visible; a separate counter records 429 attempts. Native capacity errors stop the run immediately. Bounded transport/leadership retries retain exactly the same client, sequence, operation, key and value. Wrong-owner replies refresh committed ownership without changing the request identity. An unresolved deadline stops the whole run before that VU can advance its sequence. Other VUs interrupted by abort remain interrupted, not successful or omitted.

k6's native `group_duration` measures each started `get` or `put_get` transaction through its oracle. Completed failing groups may contribute to those native distributions; failed, uncertain and interrupted operations remain separately visible in counters and receipts. There is no custom transaction timer or percentile calculation. k6 does not expose an intended-arrival timestamp for every dropped offer. Do not invent a timestamp from the ordinal of started iterations or report these tails as desired-arrival tails. Publish them with dropped/interrupted counts and the configured offered rate. Raw k6 HTTP metrics include setup; use scenario-tagged metrics and the native transaction groups for the measured path. Fixture setup and one warm read run in k6 `setup()` before the scenario.

The adapter writes bounded synthetic invocation/return receipts through k6's native console output. Retain them and native JSON samples in the task-owned run directory. Never enable HTTP debug output, upload `cluster.json`, or include capability tokens or node data in artifacts. The run is bounded to 100 transactions/s for at most 120 seconds, at most two logical data operations per transaction and at most 32 VUs. Keep the existing process/resource sampler for node and generator CPU/RSS, environment capture and cleanup; it must not schedule requests or aggregate k6 latency. The launcher must always stop only its nine owned node processes, even when setup or k6 fails.

`python tools/benchmark.py smoke` now owns nine actual nodes and one k6 subprocess, runs the full bounded fixture plus 50 mixed transactions, verifies every latest acknowledged key and every immutable read key after load, and checks cleanup. `benchmark` runs the full grid and rate/stability cells. Fault cells trigger when k6's native output records one third of planned measured starts, preserving the actual injection timestamp. Migration records committed phases and requires the target owner/epoch to settle; leader loss kills an owned group-a leader and restarts that same node/store before final readback. Pending receipts block a final durability claim because their operation may still commit. The existing real-product `tools/verify.py` also gates the hosted workflow before metrics. `profile` keeps only existing Go scheduler/GC diagnostics; native hot-path CPU profiling remains a gap.

The launcher computes no latency or throughput distribution. It retains k6's native summary and raw JSON unchanged and checks conservation of started, completed, error, dropped, unresolved and planned-but-not-started counts. Only the highest zero-loss qualifying native sweep receives the sustained run, and sustained SLO failure cannot pass the campaign. Negative matrix/rate outcomes are retained and can leave the overall verdict false.

For a separately owned, already running cluster, invoke k6 from its task-owned run directory:

```sh
CLUSTER_CONFIG=/absolute/task-owned/cluster.json K6_SUMMARY=/absolute/task-owned/k6-summary.json \
VALUE_BYTES=64 READS_PERCENT=95 \
LOAD_VUS=32 RATE=25 SECONDS=10 k6 run \
  --log-format json --out json=/absolute/task-owned/k6-samples.json \
  --console-output /absolute/task-owned/receipts.log \
  /absolute/repository/tools/benchmark-k6.js
```

Run the pure adapter contracts with `node --experimental-vm-modules tools/test_k6_adapter.cjs` and Python controls with `python -m unittest discover -s tools -p test_*.py -v`. They check routed fixture bounds, lost-return retries with unchanged identity/sequence, immediate capacity rejection, pending receipts and outcome accounting. These mocked checks establish no product performance or real-protocol proof. Next acceptance is the hosted actual nine-node smoke and full migration/leader-loss campaign. Migration/resource observations and all negative outcomes remain required before a final result can be accepted.

k6 is a standalone test tool, not a product dependency. The workflow pins the official `grafana/setup-k6-action` to `43b9fc21641a76002687994433dd586f56e791b1` and selects k6 `2.3.0`. That action downloads the versioned official GitHub release; its inspected implementation does not check an archive checksum. The launcher records the actual binary SHA-256 and version for reproducibility. No local installation or global configuration change has been made. Upstream uses AGPL-3.0; retain upstream attribution and license with any distributed tool binary. The product's dependency graph and license are unchanged.

Primary sources: [constant-arrival-rate executor](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/constant-arrival-rate/), [built-in metrics and dropped iterations](https://grafana.com/docs/k6/latest/using-k6/metrics/reference/), [test lifecycle and per-VU init](https://grafana.com/docs/k6/latest/using-k6/test-lifecycle/), [execution identifiers and abort semantics](https://grafana.com/docs/k6/latest/javascript-api/k6-execution/), and [upstream license](https://github.com/grafana/k6/blob/master/LICENSE.md). Local API evidence is `sharddb/node.go`, `sharddb/state.go` and `tools/client.py`.
