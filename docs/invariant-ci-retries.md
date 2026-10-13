# Retrying invariant CI

The PR, nightly, and weekly aggregates select evidence from the latest execution
of each required producer in the same workflow run. A successful producer that
was not rerun keeps its original attempt number. For example, test and simulator
attempt 2 may use TLA attempt 1; the log records those three original identities.
An aggregate-only retry can reuse all completed producers from that run.

Selection reads paginated GitHub job and artifact metadata, checks the run,
repository, head commit, and producer attempt, and downloads exact artifact IDs.
A failed or unfinished newer producer, missing or expired artifact, duplicate
identity, or incomplete API listing fails selection. It never falls back to an
older successful attempt. No artifact is renamed, merged, or presented as a new
execution. Failed selection also fails the final green gate.

The existing source receipts, content hashes, simulator detector artifacts,
verifier archive readback, and 44-row aggregate remain mandatory. GitHub metadata
selects the transport artifact; it does not replace evidence authentication.
The selector and downloader have read-only Actions access on aggregate jobs.
Each download receives one validated numeric ID. The pinned action uses
`merge-multiple: true` to extract that single archive directly into its layer
directory; it never receives a list or wildcard that could combine artifacts.

## Runtime installation

Runtime setup still uses the signed Ubuntu snapshot and exact package pins.
Retrieval keeps bounded retries, cached downloads, and one installation attempt;
versions and executable digests are checked before producing evidence.
Unused package translations are omitted. Setup has a 15-minute process deadline
and a 17-minute workflow step deadline, leaving time for termination and digest
reporting. Aggregate job budgets reserve time for every step and cleanup.

## Operating a retry

Wait until GitHub allows the run to be retried. After inspecting the failure,
retry failed jobs when the failure is transient and the workflow already contains
this selector. Successful TLA producers then retain their original artifacts.
For a changed commit or workflow, start normal qualification on that new source;
this helper does not import evidence from a different run or bypass any check.
An older run uses its original workflow and does not acquire these repairs.

Run the local controls with:

```sh
python3 -m unittest scripts/tests/test_invariant_runtime_setup.py scripts/tests/test_invariant_evidence.py
cargo test --locked -p rafter --test invariant_ci_contract
```

These tests exercise package-tool control flow, metadata selection, pagination,
partial reruns, negative cases, workflow wiring, and aggregate refusal. They do
not qualify Ubuntu package installation or a full hosted invariant execution.
