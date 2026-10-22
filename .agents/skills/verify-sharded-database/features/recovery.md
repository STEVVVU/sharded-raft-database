# Crash and snapshot recovery

## Sub-features
Data leader election, controller takeover, lost-quorum unavailability, durable snapshots, same-directory restart, and duplicate receipt recovery.

## How to get to it (user POV)
Restart a failed node with its original ID, fixed configuration, and state directory.

## Driving it with Python
Run `python tools/verify.py`. The driver records and kills owned PIDs, proves continued committed reads after one data leader fails, snapshots each group, restarts all nine processes, and checks actual snapshot restore logs in all groups. It removes a data quorum, records the uncertain append, restores those processes, and resolves the same identity without a repeated append.

## Gotchas
Never reuse a data directory for another node. Evidence and logs remain after process cleanup. Windows race detection needs an unavailable C compiler and is not claimed by this driver.
