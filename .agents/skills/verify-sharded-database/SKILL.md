---
name: verify-sharded-database
description: Drive the real nine-process sharded database and retain fault evidence.
---

## Launch
Run `go build -o sharded.exe ./cmd/sharded`, then `python tools/verify.py`. The driver owns nine subprocesses, isolated directories and loopback ports. No external service is used.

## Doctor
The driver authenticates `/status`, verifies all owned PIDs are live and waits for a committed controller query and both data groups.

## Drive
`python tools/verify.py` drives initialization, overlapping writes and reads, migration, duplicate retry, leader failure, snapshot and same-directory restart. See features/README.md. Baseline onboarding uses `go build -o baseline.exe .` then `python tools/verify.py --baseline` and has been executed successfully.

## Evidence
JSON invocation/return histories, assertions and per-process logs remain under `evidence/<run>/`. Only successful HTTP commits count as acknowledged; timeouts remain indeterminate. State directories are separate from logs.

## Cleanup
The driver terminates and waits only for PIDs it created in a finally block. Evidence survives cleanup. State remains under the run folder for inspection.
