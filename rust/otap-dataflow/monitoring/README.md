# Local Prometheus + Grafana for `df_engine`

A turnkey monitoring stack for the OTAP dataflow engine. Spins up
Prometheus + Grafana in Docker and ships a pre-built dashboard that polls
`df_engine`'s admin API and renders the pipeline / exporter metrics that
aren't surfaced in the built-in admin UI.

## Prerequisites

You need either:

- **Docker Desktop** for Windows / macOS, **or**
- **Docker Engine inside WSL2** (Ubuntu) — see the WSL section below.

`df_engine.exe` runs natively on the host (Windows), not inside Docker.

## Start (Docker Desktop)

From the repository root:

```powershell
docker compose -f monitoring/docker-compose.yml up -d
```

## Start (Docker Engine in WSL2)

If your Docker daemon runs inside WSL2 Ubuntu rather than Docker Desktop,
the compose file works the same way — you just invoke it through `wsl`.
You also need to bind `df_engine`'s admin server to `0.0.0.0` so it's
reachable from the WSL2 virtual network (default `127.0.0.1` is loopback
only and won't accept connections from WSL containers).

In one PowerShell window, start the engine bound to all interfaces:

```powershell
.\target\release\df_engine.exe `
    --config fakegen-geneva.yaml `
    --num-cores 1 `
    --http-admin-bind 0.0.0.0:8080
```

In another, start the stack:

```powershell
wsl -- bash -c "cd /mnt/c/otel-arrow/rust/otap-dataflow && docker compose -f monitoring/docker-compose.yml up -d"
```

If Windows Defender Firewall blocks inbound port 8080 from WSL,
you'll see Prometheus targets stuck on `DOWN`. Allow it once with:

```powershell
# Run as admin
New-NetFirewallRule -DisplayName "df_engine admin (WSL)" `
    -Direction Inbound -Protocol TCP -LocalPort 8080 `
    -Action Allow -Profile Any
```

## Open

- Grafana:    <http://localhost:3000>  (anonymous Admin access — no login required)
- Prometheus: <http://localhost:9090>

The default dashboard is **Geneva exporter throughput** and it auto-refreshes
every 5 seconds.

## Stop

```powershell
# Docker Desktop
docker compose -f monitoring/docker-compose.yml down

# WSL2
wsl -- bash -c "cd /mnt/c/otel-arrow/rust/otap-dataflow && docker compose -f monitoring/docker-compose.yml down"
```

This preserves Prometheus + Grafana data in named volumes
(`prometheus_data`, `grafana_data`). Add `-v` to also drop them.

## What's on the dashboard

Just the metric you asked for:

- **Stat tile**: current Geneva export rate as a single big number with
  red / yellow / green thresholds.
- **Time series**: the same rate as a continuous chart over the last 15 min.

Both come from `sum(rate(log_records_uploaded{set="otap.exporter.geneva"}[30s]))`.

## How it works

`df_engine`'s admin endpoint exposes Prometheus text format at:

```text
http://127.0.0.1:8080/api/v1/telemetry/metrics?format=prometheus
```

[`prometheus.yml`](prometheus.yml) scrapes this every 2 seconds. From the
container's perspective the host is reachable as `host.docker.internal:8080`.
Both Docker Desktop and WSL Docker resolve this via the
`extra_hosts: host-gateway` mapping in `docker-compose.yml`.

Grafana auto-loads:

- The `Prometheus` datasource ([provisioning/datasources/prometheus.yml](grafana/provisioning/datasources/prometheus.yml))
- The dashboard ([dashboards/df_engine.json](grafana/dashboards/df_engine.json))
  ([provisioning/dashboards/dashboards.yml](grafana/provisioning/dashboards/dashboards.yml))

## Troubleshooting

**No data in panels?**

1. Confirm `df_engine` is running and exposing the admin endpoint:

   ```powershell
   (Invoke-WebRequest http://127.0.0.1:8080/api/v1/telemetry/metrics?format=prometheus -UseBasicParsing).Content `
       -split "`n" | Select-Object -First 5
   ```

2. In Prometheus → **Status → Targets**, check that the `df_engine` job is
   `UP`. If it shows `DOWN` with a connection error:
   - Docker Desktop: ensure Docker Desktop is fully started.
   - WSL Docker: ensure `df_engine` is bound to `0.0.0.0:8080` (use
     `--http-admin-bind 0.0.0.0:8080`) and the Windows firewall isn't
     blocking inbound port 8080.

3. The Geneva-specific panels stay empty until the engine actually uploads
   to Geneva. If you're using a noop exporter or running offline, expect
   Geneva charts to be flat.

**Dashboard panel missing or stuck on "loading"?**

Restart just Grafana to re-provision:

```powershell
docker compose -f monitoring/docker-compose.yml restart grafana
# or, in WSL
wsl -- bash -c "cd /mnt/c/otel-arrow/rust/otap-dataflow && docker compose -f monitoring/docker-compose.yml restart grafana"
```
