# Geneva Exporter — A/B Comparison: Baseline vs Transform

Two YAML configs live in the workspace root for the comparison:

- [fakegen-geneva.yaml](../fakegen-geneva.yaml) — generator → Geneva (baseline)
- [fakegen-geneva-transform.yaml](../fakegen-geneva-transform.yaml) —
  generator → transform → Geneva

Both configs make the traffic generator emit log records with **50 attributes
each** (`num_log_attributes: 50`). Each scenario tags its records with a
distinct `event_name` so they can be told apart in Geneva:

| Scenario           | YAML                              | `event_name` |
| ------------------ | --------------------------------- | ------------ |
| Baseline (A)       | `fakegen-geneva.yaml`             | `Test1`      |
| With transform (B) | `fakegen-geneva-transform.yaml`   | `Test2`      |

The transform processor in scenario B **renames** all 50 attributes by
prefixing every key with `renamed.` (e.g. `thread.id` → `renamed.thread.id`).
The column count is unchanged — only the keys differ. So the difference in
CPU between the two runs is the cost of the transform performing 50
`project-rename` operations per record.

## Common columns (sent to Geneva in both scenarios)

| Column                    | Source           | Value                                             |
| ------------------------- | ---------------- | ------------------------------------------------- |
| `time_unix_nano`          | LogRecord        | wall-clock at generation                          |
| `observed_time_unix_nano` | LogRecord        | same value                                        |
| `severity_number`         | LogRecord        | 9 / 13 / 17 (INFO / WARN / ERROR, ~80/15/5 % mix) |
| `severity_text`           | LogRecord        | `"INFO"` / `"WARN"` / `"ERROR"`                   |
| `body`                    | LogRecord        | one of ~50 generic strings, cycling               |
| `event_name`              | LogRecord        | `"Test1"` (baseline) / `"Test2"` (transform)      |
| `trace_id` / `span_id`    | LogRecord        | _empty_ (`use_trace_context` is false)            |
| `flags`                   | LogRecord        | _empty_                                           |
| `service.name`            | Resource         | `"load-generator"`                                |
| `service.version`         | Resource         | `"1.0.0"`                                         |
| `service.instance.id`     | Resource         | `"instance-001"`                                  |
| `scope.name`              | Instrumentation  | `"fake_signal"`                                   |
| `scope.version`           | Instrumentation  | `"1.0.0"`                                         |

## Scenario A — Baseline (`fakegen-geneva.yaml`)

In addition to the common columns above, **all 50 attributes** the generator
produces are sent to Geneva. The names follow OTel semantic conventions;
representative examples:

- `thread.id`, `thread.name`, `code.function`, `code.namespace`,
  `code.filepath`, `code.lineno`, `log.record.uid`, `event.name`
- `exception.type`, `exception.message`, `exception.stacktrace`
- `user.id`, `user.name`, `user.email`, `session.id`
- `http.request.method`, `http.response.status_code`, `http.route`,
  `url.full`, `url.path`, `url.scheme`
- `server.address`, `server.port`, `client.address`, `client.port`,
  `network.protocol.name`, `network.protocol.version`, `network.transport`
- `db.system`, `db.namespace`, `db.operation.name`, `db.query.text`,
  `db.collection.name`
- `messaging.system`, `messaging.operation.type`,
  `messaging.destination.name`, `messaging.message.id`
- `rpc.system`, `rpc.service`, `rpc.method`
- `enduser.id`, `enduser.role`, `enduser.scope`
- `cloud.provider`, `cloud.region`, `cloud.availability_zone`,
  `cloud.account.id`
- `container.id`, `container.name`, `container.image.name`

So per record: **13 common columns + 50 attributes = 63 columns**.

## Scenario B — With transform (`fakegen-geneva-transform.yaml`)

The transform processor runs a KQL query that lists all 50 attribute keys
inside a single `project-rename`, prefixing each with `renamed.`:

```text
logs | project-rename
    attributes["renamed.thread.id"]   = attributes["thread.id"],
    attributes["renamed.thread.name"] = attributes["thread.name"],
    …
    attributes["renamed.container.image.name"] = attributes["container.image.name"]
```

The result: all 50 attributes still arrive at Geneva, but their keys are
prefixed with `renamed.`. Column count is unchanged.

So per record: **13 common columns + 50 renamed attributes = 63 columns**.

## Quick diff

| Scenario           | Common cols | Attributes | Total columns | `event_name` | Attribute keys                |
| ------------------ | ----------- | ---------- | ------------- | ------------ | ----------------------------- |
| Baseline (A)       | 13          | 50         | 63            | `Test1`      | as emitted (e.g. `thread.id`) |
| With transform (B) | 13          | 50         | 63            | `Test2`      | prefixed (e.g. `renamed.thread.id`) |

## Running the A/B

Run each side for the same wall-clock duration so the only meaningful
difference is whether the transform is in the path.

```powershell
.\target\release\df_engine.exe --config fakegen-geneva.yaml           --num-cores 1 --run-duration-secs 120
.\target\release\df_engine.exe --config fakegen-geneva-transform.yaml --num-cores 1 --run-duration-secs 120
```

Each run prints a `=== Run summary ===` block on exit. The number to look at
is **`cpu_time per record`** (in nanoseconds).

## CPU metric — what we measure and why

`cpu_time per record` answers: _how much CPU time does the engine spend per
log record?_ It is computed once per run as

```text
cpu_time per record  =  Δcpu_seconds × 1e9 / Δlog_records_uploaded
```

where the deltas are taken between the very first and very last snapshot
of the run.

### Where the inputs come from

- **`Δcpu_seconds`** — `cpu_time::ProcessTime::elapsed()` between the start
  and end of the run. On Windows that's a thin wrapper around
  [`GetProcessTimes`](https://learn.microsoft.com/windows/win32/api/processthreadsapi/nf-processthreadsapi-getprocesstimes),
  which sums **user + kernel** CPU time used by `df_engine.exe` and only by
  `df_engine.exe`. Anything Chrome, VS Code, or other apps do on the box is
  not counted.
- **`Δlog_records_uploaded`** — the cumulative
  `log_records_uploaded{set="otap.exporter.geneva"}` counter from the admin
  endpoint at `http://127.0.0.1:8080/api/v1/telemetry/metrics/aggregate`. The
  Geneva exporter increments this once per log record successfully sent.

Both counters are monotonic and incremented at the actual moment the work
happens, so the ratio is a true seconds-per-record cost.

### Why a single end-to-end delta is the right shape

A naive alternative — sample the counters every 5 s and compute
`Δcpu / Δrecords` per tick — has two problems with a low-CPU workload:

- The Windows scheduler quantum is ~15.6 ms; `GetProcessTimes` only updates
  in those quanta. At ~3 ms of CPU per second, each 5-s tick consumes ~15 ms
  of CPU — exactly one quantum. The reading lands on either 0 ms or 15.6 ms,
  giving wildly different per-tick `ns/log` values.
- Per-tick startup costs (the first batch's TLS handshake, the Geneva
  ingestion-info fetch, JIT-style warmup) bias short windows downward or
  upward depending on which tick they land in.

Taking one delta over the whole run cancels both effects:

- The quantum rounding error is **fixed in absolute terms** at ~±15.6 ms.
  As the run gets longer the relative error shrinks linearly:

  | Run length | CPU consumed | Quantum noise | Relative error in `ns/log` |
  | ---------- | ------------ | ------------- | -------------------------- |
  | 30 s       | ~190 ms      | ±15.6 ms      | **±8 %**                   |
  | 60 s       | ~380 ms      | ±15.6 ms      | ±4 %                       |
  | 120 s      | ~760 ms      | ±15.6 ms      | ±2 %                       |
  | 300 s      | ~1.9 s       | ±15.6 ms      | ±0.8 %                     |

- One-off startup costs are amortized across all the records produced
  during the run, not just the few in a single tick.

### Why this is fair for an A/B comparison

`cpu_time` is **only** the CPU `df_engine.exe` itself spent — it does not
include time spent waiting for the network. The Geneva uploads are I/O bound
(round-trip ~30 ms on PPE), so any wall-clock variance in network response
time changes how many records flow through but does **not** inflate
`cpu_time`. Dividing by `log_records_uploaded` then normalizes out that
throughput variance.

The result: even though both runs may upload slightly different numbers of
records (because network jitter changes throughput), `cpu_time per record`
isolates the engine's CPU cost per unit of work.

### Computing the transform's per-record cost

```text
transform_cost_per_record =
    (cpu_time per record)_with_transform - (cpu_time per record)_baseline
```

If the baseline reports e.g. `6 466 ns/log` and the transform run reports
`8 200 ns/log`, then renaming 50 attributes via `project-rename` costs
**~1 700 ns ≈ 1.7 µs per record** on this hardware.

### Practical guidance

- Run each side for at least **120 s** for a confident number; 60 s is the
  minimum that still gives single-digit-percent error.
- Run the baseline twice and confirm the two `ns/log` values agree to
  within a few percent before trusting the diff against the transform run.
  If they don't, the noise floor is too high and you need a longer run.
- Both YAMLs intentionally use the **same** `num_log_attributes: 50` so the
  generator emits an identical input shape — only the transform changes
  between the two runs.
