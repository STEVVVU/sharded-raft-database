# Frozen real-node benchmark

Run `python -m unittest discover -s tools -p test_*.py -v` then `python tools/benchmark.py smoke`. Smoke owns nine actual nodes, seeds four fixture keys and checks four mixed committed operations, exact return values and every acknowledged write by readback. Its timings prove instrumentation only.

`python tools/benchmark.py benchmark` requires isolated Linux. Version1 freezes seed421, 1024 immutable read-fixture keys, 64/4096-byte values, read percentages100/95/50/0 and concurrency1/8/32. All24 cells get five repetitions, reversing order. Baseline is concurrency1 with identical mix/data. Writes use unique keys and identities; reads use immutable fixture keys. Exact echo and post-timing readback prove accepted writes. This disjoint-key load does not establish contention or universal linearizability. Fixture setup/readback are outside operation timing; one untimed fixture read establishes warmth.

Open-loop sweeps offer5/25/100ops/s for10s with32 outstanding slots; declines remain explicit. Highest zero-loss rate with dispatch-p99<100ms and operation-p99<5s gets120s stability. Separate traffic cells inject observed shard migration and group-a leader loss with same-store restart. Migration phases/owners and settled time are retained. Client retry time stays inside scheduled-arrival latency; unresolved writes retain indeterminate identities/sequences.

Ignored `evidence/benchmark-*/` holds source/dirty/environment, workload/input hash, all outcomes/history, per-type tails, node status, migration observations and tracked-node/generator Linux CPU/RSS every100ms. CPU ticks use retained CLK_TCK. /status exposes no commit/applied indexes, so replication lag is unmeasured. Fixed four-shard membership precludes shard-count scaling. Separate `profile` mode gives generator cProfile and Go scheduler/GC, not native hot-path CPU profiles. Capture service CPU before cause-dependent fixes.

All processes and observer threads are owned and cleanup checked. Evidence survives. Manual read-only Ubuntu workflow excludes configs/capabilities/state from artifacts. No measured performance result is yet published.

