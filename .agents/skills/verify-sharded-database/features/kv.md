# Replicated key operations

## Sub-features
Get, Put, Append, bounded receipts, leader retries, and per-key linearizable histories.

## How to get to it (user POV)
Use tools.client.Client.op with a stable serialized client identity. The client refreshes committed configuration and routes to the current owner.

## Driving it with Python
Run `python tools/verify.py`. Both groups receive Put commands. Two writers and one reader overlap on a moving shard. The final oracle checks all acknowledged appends and the independent history search checks per-key real-time ordering.

## Gotchas
One outstanding request per client. Missing keys return an empty string. Capacity errors do not evict receipts. Uncertain operations must resolve through same-identity retry before this bounded completed-history checker can pass.
