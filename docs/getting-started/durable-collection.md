# Bring your collection function

This repository example connects your synchronous collection function to ECF's
existing durable queue, retry policy and SQLite dead-letter queue (DLQ). It adds
no framework API and requires no browser or external queue service.

## Install and run

Clone this repository and run `python -m pip install .` from its root. Installing
the PyPI wheel alone does not give you the repository's example files.

For a self-contained introduction, run:

```bash
python examples/event_pipeline/failure_recovery_demo.py
```

This starts a loopback HTTP fixture on an available port and runs the collection
CLI in separate processes. It checks success, HTTP 503 retry across process
boundaries, empty-data rejection, HTTP 429 batch stopping, and selective replay.
The fixture simulates operator review by explicitly allowing the limited route
before replay; this is not automatic production rate-limit recovery. Retry delay
is zero to keep the demonstration short; the worker still persists retry state.

The printed temporary directory retains SQLite databases, results and
`demo-report.json` for inspection. Each run creates a new directory and never
resets existing state. Remove that specific directory yourself when finished.
The fixture stops when the demo exits, so its saved URLs cannot be replayed
afterwards. The demo exits 0 only after its checks pass, even though its worker
commands exit 2 when historical DLQ records remain. Use `--pace 8` to leave time
to read each step while recording your terminal.

To work through the commands yourself with a persistent local server:

For a local, repeatable success case, create a directory containing `item.json`
with the contents `{"value": 42}`. In a separate terminal, serve that directory:

```bash
python -m http.server 8000 --bind 127.0.0.1 --directory /path/to/fixture
```

Replace the directory with your actual path (quote paths containing spaces).
From the repository root, run:

```bash
python examples/event_pipeline/collect_json.py --state-dir /path/to/ecf-state enqueue item-1 http://127.0.0.1:8000/item.json
python examples/event_pipeline/collect_json.py --state-dir /path/to/ecf-state work
python examples/event_pipeline/collect_json.py --state-dir /path/to/ecf-state inspect --job-id item-1
```

Use the same state directory on every invocation. It contains `queue.sqlite3`,
`dlq.sqlite3` and `results/`. The successful worker reports `processed_count: 1`
and `active_count: 0`. Inspect reports `job: null` after ACK and the output path.
The file contains `{"job_id": "item-1", "data": {"value": 42}}`.

To exercise retry, enqueue a second job, stop the HTTP server, then run `work`.
The connection error schedules a retry: inspect the job to see `state: pending`,
`delivery_count: 1` and `next_attempt_at`. Restart the HTTP server, wait at least
five seconds, and invoke `work` in a new process. It opens the same database and
can complete delivery #2. Do not enqueue the same job again to resume it.

To exercise terminal failure, enqueue a URL ending in `/missing.json` while the
server is running. A 404 goes to SQLite DLQ on the first delivery. `inspect`
shows the persisted error type, message, job payload and delivery count.

## Review and rerun only the failed job

After fixing the source or collection code, copy the `dead_letter_id` from
`inspect` and publish that one job with a **new** ID:

```bash
python examples/event_pipeline/collect_json.py --state-dir /path/to/ecf-state replay DEAD_LETTER_ID --new-job-id item-1-reviewed
python examples/event_pipeline/collect_json.py --state-dir /path/to/ecf-state work
python examples/event_pipeline/collect_json.py --state-dir /path/to/ecf-state inspect --job-id item-1-reviewed
```

For the local 404 example, create `missing.json` in the served directory before
replay. The successful jobs are not re-enqueued. The new job gets a fresh delivery
budget and its own output filename; its event metadata records `replayed_from`
and `original_message_id`. The original DLQ record remains available for review.
Consequently `work` still exits 2 while historical DLQ records exist; inspect its
batch summary and the new job's output to see whether recovery succeeded.

Replay refuses unknown DLQ IDs, active original jobs, and IDs already present in
the queue, DLQ or result directory. This protects the single-worker example from
accidental ID reuse and replay after an incomplete source finalization. It is
not a distributed replay transaction or a global replay-once guarantee: an
operator can intentionally replay again with another new ID. Stop workers while
reviewing/replaying; the preflight checks do not lock all stores together.

## Failure policy in this example

| Failure | Behavior | Operator action |
| --- | --- | --- |
| Connection/timeout, HTTP 408/500/502/503/504 | Scheduled retry, then DLQ after exhaustion | Inspect persisted delivery state; invoke work again when due |
| HTTP 429 | Preserve error and Retry-After in DLQ; stop this batch | Review source limits, wait as required and adjust collection before explicit replay |
| Null, empty object/list/string or blank string | DLQ before output is written | Inspect source/parser; fix it before replay |
| Other HTTP errors, invalid JSON, output write failure | DLQ | Diagnose and fix the cause before replay |

HTTP 200 alone is not evidence of useful data. The default `validate_data`
rejects empty results while accepting valid `0` and `false`. A previous output
file is left untouched on validation failure; it may therefore be stale. Check
DLQ and exit status instead of treating an existing file as proof of freshness.
For sources where empty data is legitimate, enqueue with `--allow-empty`.
Replace `validate_data` with source-specific schema/completeness checks:
`{"items": []}` is not detected by this basic top-level check.

429 is deliberately **not** automatically retried. Stopping the batch leaves
other jobs pending, but is not a persisted domain-wide cooldown: starting a new
worker can process them. Review before restarting workers. Retry-After is shown
as received, not parsed or enforced. No automatic client/proxy switching is
performed. If your application needs another recovery path, implement it in
your collection layer after understanding the source's requirements.

## Connect your code

Edit `collect(session, url)` in the example. Return JSON-serializable data on
success and raise an exception on failure. This sample stores one result per
job ID with ECF's atomic writer. The caller creates and closes the session;
`collect` must not close an injected client.

For a browser or custom client, use the same ownership rule: create it around
the worker, pass it to your handler, and close it after the worker finishes.
This is an integration pattern, not an included browser adapter, browser pool,
crash watchdog or async runtime. Browser-specific exceptions need an explicit
application policy. Keep framework-installed dependencies unchanged.

These are example choices, not universal framework policies. Defaults are three
deliveries and exponential delays starting at five seconds.
`work --retry-delay 10 --max-deliveries 4` changes them. Requests uses its default adapter with no
additional transport retries; using ECF's HTTP session would add a separate
transport retry layer. Budget those layers separately.

## ExecutionResult and handler exceptions are different contracts

Do **not** use `lambda event: bot.run()` as a failure-aware job adapter.
`BaseBot.run()` turns execution exceptions into `ExecutionResult`. The dispatcher
treats a normal handler return as success, even if that value is a FAILED result.
The worker can therefore ACK that delivery. This example calls collection
directly so the original exception reaches RetryPolicy.

If you compose a BaseBot with a worker, define an application adapter explicitly:

| Bot result | Required application decision before completing the delivery |
| --- | --- |
| COMPLETED | ACK only after all required output has been committed. |
| FAILED | Route failure explicitly; never silently return the result as success. |
| CANCELLED | Decide whether the unfinished job remains recoverable; do not classify cancellation as an ordinary retryable error by default. |
| DEGRADED | Decide whether the useful result is complete enough; automatic retry can repeat already committed side effects. |
| SKIPPED | ACK only if skipping satisfies the logical job (for example, a verified existing result). |
| INITIALIZED / RUNNING | Not terminal outcomes; do not treat them as completed work. |

Failure metadata includes a type name and message, not the original exception
object. Raising a generic RuntimeError from it loses type-based classification.
There is deliberately no generic BaseBot adapter in this example. Do not bypass
lifecycle by calling `execute()` directly or overriding `run()` to solve this.

## Operation and limits

- `work` processes a batch. It stops after a retry decision, HTTP 429, or when no due job
  remains. It does not wait for scheduled retries or act as a cron scheduler.
  Exit code 0 means no active jobs, no DLQ records and no failures in this batch;
  2 means attention or another invocation is needed. Old DLQ records also keep
  the exit code at 2. `inspect` itself exits 0 and does not replay/delete jobs.
- Ctrl+C/SIGTERM requests a stop after the current handler. It does not cancel
  an in-flight HTTP request. Handlers must finish before resources close.
  Request timeouts are connect/read inactivity limits, not total job deadlines.
- Use one worker process for this introductory example. `--lease-seconds 300`
  (before the subcommand) sets its fixed lease. There is no automatic lease
  renewal. If another worker recovers an expired lease while the first handler
  is still running, both can execute the job. Choose bounded jobs and a lease
  longer than their expected duration; a larger lease delays crash recovery.
- Successful output is written before ACK. A crash between write and ACK can
  repeat collection. Stable output filenames and atomic replacement avoid
  duplicate appended records, but do not provide exactly-once side effects or
  guarantee identical data if the source changes. Reusing a job ID after ACK
  is allowed and replaces that result. Use a new ID for a separate observation.
- Retry state survives restart; it is not a full attempt history. Worker counters
  are process-local, while DLQ records persist terminal failure information.
  Inspecting an unknown/ACKed ID returns null; that alone is not proof of success.
- The demo does not automatically save HTML/screenshots. Add source-specific
  artifact capture deliberately; payloads, URLs and errors can contain secrets.
- Queue recovery is not pagination checkpointing. `LocalStateStore.put/get`
  can hold a source-specific cursor, and `has_seen/mark_seen` can track records.
  Save output before advancing a checkpoint. These operations are not one
  transaction with a remote sink and are not a distributed idempotency lock.

The tests in `tests/integration/test_durable_collection_example.py` run a local
HTTP server and separate CLI processes to cover restart, scheduling, DLQ,
rate-limit stopping, empty-data rejection, selective replay and output failure
without depending on public websites.
