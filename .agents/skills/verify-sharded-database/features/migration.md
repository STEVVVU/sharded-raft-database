# Shard migration

## Sub-features
Durable intent, frozen donor, staged receiver, published configuration, activation, cleanup, and controller fencing.

## How to get to it (user POV)
Send POST /move to the controller leader with the next epoch and expected owner. Its elected leader resumes the durable phases automatically.

## Driving it with Python
Run `python tools/verify.py`. The driver moves a shard while clients run, kills the real controller leader after an intermediate phase, waits for its successor to finish, and moves the shard back. Competing valid intent, stale client epoch, and delayed cleanup must return the specific expected rejection.

## Gotchas
Only one migration is admitted at a time. A shard is temporarily unavailable while ownership changes. Epoch tombstones must survive cleanup and snapshot restore.
