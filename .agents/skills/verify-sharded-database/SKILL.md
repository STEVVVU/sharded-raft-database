---
name: verify-sharded-database
description: Drive the real nine-process sharded database and retain fault evidence.
---

## Launch
Run `go build -o sharded.exe ./cmd/sharded`, then `python tools/verify.py`. The driver owns nine subprocesses, isolated directories and loopback ports. No external service is used.

## Doctor
The driver authenticates `/status`, verifies all owned PIDs are live and waits for a committed controller query and both data groups.

## Drive
`python tools/verify.py` drives initialization, overlapping writes and reads, migration, duplicate retry, leader failure, snapshot and same-directory restart. It also holds a real cleanup response in an owned authenticated loopback proxy, captures a retired donor snapshot while controller phase 4 remains committed, kills that controller, and proves cleanup retry and another migration finish. The fault barrier changes no product code or shared state. See features/README.md. Baseline onboarding uses `go build -o baseline.exe .` then `python tools/verify.py --baseline` and has been executed successfully.

## Evidence
JSON invocation/return histories, assertions, retained cleanup-donor snapshots and per-process logs remain under `evidence/<run>/`. Only successful HTTP commits count as acknowledged; timeouts remain indeterminate. State directories are separate from logs.

Every journey initializes `result.json` with `passed: false`. It records any exception and finalizes the verdict only after owned cleanup, including process and proxy-thread status. `python tools/verify.py --fail-after-doctor` deliberately raises after all nine nodes become ready. Expect exit 1, retained error, `passed: false`, and both cleanup status fields true. This is a verification-only failure flag; it changes no product behavior. CI checks this path before the normal journey. Historical evidence is never retroactively rewritten.

## Cleanup
The driver terminates and waits only for PIDs it created in a finally block. It releases its response barrier, closes its proxy server, and joins its owned server thread. Evidence survives cleanup. State remains under the run folder for inspection.
