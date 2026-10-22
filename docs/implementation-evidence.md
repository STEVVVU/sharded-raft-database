# Local implementation evidence

The current driver verdict evidence is `evidence/journey-1792633010718233200`, which passes all 37 logical operations and reports successful process and proxy cleanup in its finalized result. A controlled failure is retained at `evidence/journey-1792633075169719800`. `python tools/verify.py --fail-after-doctor` exits 1 with the original RuntimeError, `passed: false`, stopped owned processes and proxy threads, and no cleanup errors. The exact CI failure-verdict assertions were executed locally and passed. The baseline was also rerun at `evidence/baseline-1792633053384438600`, with success finalized after its owned process stopped.

The prior driver wrote success before cleanup and did not write a result on failure. Its historical red journey `journey-1792632535071971900` therefore has events and history but no result.json. That folder remains unchanged. New journeys initialize a false verdict, retain errors, and finalize only after actual cleanup. Cleanup failures cannot retain `passed: true`, and the original failure still exits nonzero. CI now exercises the controlled failure path before the normal journey.

Upstream base `97a01648bd9e2dfc4c95c3c25a3621dd8d153adb` was cloned with its complete history on 2026-10-21. The extension preserves the original store, HTTP package, executable, license, and upstream README.

The first baseline result is retained in `evidence/baseline-1792631500266386700`. It records an actual original executable HTTP write/read and owned-process cleanup. The initial sharded process journey is retained in `evidence/journey-1792631689482692300`. Later journeys add overlapping readers and two writers and correct the competing-intent fixture to require the specific `migration busy` error.

The meaningful dependency change is x/sys v0.21.0 to v0.44.0. `govulncheck v1.9.0` initially identified GO-2026-5024 at package level with no reachable symbol, and reported no vulnerabilities after the update. No dependencies were added for the sharded protocol or Python driver.

The initial completed nine-process result is retained in `evidence/journey-1792632136325424700`. It passed 37 distinct logical operations after resolving the uncertain request through its original identity. The event record includes a controller crash during a durable intermediate phase, a `migration busy` rejection for a competing valid intent, stale cleanup rejection after reacquisition, a real data leader crash, lost data quorum, snapshot restore observations in all three groups, and all owned processes stopped.

Independent review then found that donor cleanup was not idempotent after its commit. The first cleanup changed `frozen` to `retired`; repeating its exact epoch returned `stale cleanup`, so a lost response could strand controller phase 4. A new focused regression failed before the fix with `retry after committed cleanup must succeed, got "stale cleanup"`. A real old-binary reproduction is retained in `evidence/journey-1792632535071971900`. An owned authenticated proxy withheld the successful cleanup reply. A real donor snapshot proved retirement while a committed controller query showed phase 4. After a controller crash, the successor remained at phase 4 for the bounded observation and the journey failed. All owned processes were cleaned up.

Cleanup now accepts `frozen` or `retired` only with the same exact expected epoch. Other epochs and cleanup after reacquisition still fail. The regression checks both live state and snapshot restore. The final corrected nine-process result is retained in `evidence/journey-1792632704578729100`. It passed 37 logical operations, the controlled crash after cleanup and before phase 5, successful successor cleanup retry, a subsequent migration, stale cleanup rejection, and all earlier fault journeys. Its retained `cleanup-donor-*.json` snapshot still exists after cleanup. All three donor HTTP addresses use owned proxies sharing one response barrier, so RPC timeout and peer fallback cannot bypass that boundary. Product code has no fault hook or delay.

One initial barrier-driver attempt, `evidence/journey-1792632427657694600`, failed because it killed a second controller before restarting the first, causing a separate quorum loss. That driver sequencing error was corrected before the decisive old-binary reproduction. Failed attempts remain retained and are not counted as passing evidence.

Final local commands passed on 2026-10-21 with Go 1.27.3 and Python 3.11:

```text
go test ./...
go vet ./...
python -m unittest discover -s tools -p test_*.py
go build -o sharded.exe ./cmd/sharded
python tools/verify.py
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
git diff --check
```

Generated evidence and state are ignored by Git and retained locally. They are not source fixtures. The GitHub workflow uploads history, event, verdict, log files, and the specific retained retired-donor snapshot; it excludes capability configuration and live node state directories. The workflow now uses checkout v7, setup-go v7, setup-python v6, and upload-artifact v6. The `.gitattributes` file enforces LF for Go sources; `gofmt -l .` returns no paths, and the original example's indexed source remains unchanged. Hosted CI and the race detector have not run. This environment has no C compiler for Windows race instrumentation. No benchmark or all-executions safety proof is claimed.
