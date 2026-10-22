# Local implementation evidence

Upstream base `97a01648bd9e2dfc4c95c3c25a3621dd8d153adb` was cloned with its complete history on 2026-10-21. The extension preserves the original store, HTTP package, executable, license, and upstream README.

The first baseline result is retained in `evidence/baseline-1792631500266386700`. It records an actual original executable HTTP write/read and owned-process cleanup. The initial sharded process journey is retained in `evidence/journey-1792631689482692300`. Later journeys add overlapping readers and two writers and correct the competing-intent fixture to require the specific `migration busy` error.

The meaningful dependency change is x/sys v0.21.0 to v0.44.0. `govulncheck v1.9.0` initially identified GO-2026-5024 at package level with no reachable symbol, and reported no vulnerabilities after the update. No dependencies were added for the sharded protocol or Python driver.

The final nine-process result is retained in `evidence/journey-1792632136325424700`. It passed 37 distinct logical operations after resolving the uncertain request through its original identity. The event record includes a controller crash during a durable intermediate phase, a `migration busy` rejection for a competing valid intent, stale cleanup rejection after reacquisition, a real data leader crash, lost data quorum, snapshot restore observations in all three groups, and all owned processes stopped.

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

Generated evidence and state are ignored by Git and retained locally. They are not source fixtures. The GitHub workflow uploads only history, event, verdict, and log files; it excludes capability configuration and durable state. Hosted CI and the race detector have not run. This environment has no C compiler for Windows race instrumentation. No benchmark or all-executions safety proof is claimed.
