# Features

- [Replicated key operations](kv.md).
- [Shard migration](migration.md).
- [Crash and snapshot recovery](recovery.md).
- [Frozen read/write load and migration benchmark](benchmark.md).

## Sub-features
Replicated Get/Put/Append, durable duplicate suppression, fenced shard migration, controller recovery, quorum failure, snapshots and restart.

## How to get to it (user POV)
Start fixed three-member controller and two fixed three-member data groups with `sharded -config cluster.json -node NODE`.

## Driving it with Python
Run `python tools/verify.py`. The client uses authenticated HTTP, follows bounded leader retries and refreshes the committed shard configuration.

## Gotchas
Four fixed shards, two groups, loopback single-tenant deployment. No transactions across shards. No race result without a C compiler.
