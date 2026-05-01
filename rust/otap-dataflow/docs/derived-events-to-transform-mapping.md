# Derived Event → `transform_processor` mapping examples

A few notes about syntax first:

- DE `<Query>` references **flat columns** (`Level`, `Message`, `RoleInstance`, …). In OTAP, a log record has well‑known fields (`Timestamp`, `ObservedTimestamp`, `SeverityNumber`, `SeverityText`, `Body`, `TraceId`, `SpanId`) and a bag of `attributes["key"]` (plus resource/scope attributes). Most "columns" map to `attributes["..."]`.
- DE `<Query>` is implicitly scoped to one source; in OTAP you put `logs |` (or `metrics |` / `traces |`) at the front.
- DE `eventName` becomes "what node consumes the output" — not part of the query.
- The transform processor is row‑level: `extend`, `project`, `project-rename`, `project-away`, `where`, plus scalars (`case`, `strcat`, `iif`, `tolower`, `tostring`, `tolong`, `substring`, `replace_string`, …). It **does not** do `summarize`/`bin()`.

---

## 1. Pure rename of one column

**DE**

```xml
<DerivedEvent source="MyEvent" eventName="MyEventRenamed"
              storeType="Local" duration="PT1M">
  <Query>extend NewName = OldName | project-away OldName</Query>
</DerivedEvent>
```

**OTAP** (use `attributes_processor` for the cheap path, but here is the transform form)

```yaml
rename:
  type: "urn:otel:processor:transform"
  config:
    kql_query: |
      logs | project-rename attributes["NewName"] = attributes["OldName"]
```

## 2. Bulk rename of many columns

**DE**

```xml
<DerivedEvent source="ThreadEvent" eventName="ThreadEventRenamed"
              storeType="Central" duration="PT1M">
  <Query>
    extend ThreadId = OldThreadId, ThreadName = OldThreadName, CpuMs = OldCpuMs
    | project-away OldThreadId, OldThreadName, OldCpuMs
  </Query>
</DerivedEvent>
```

**OTAP** (this is the shape used in `fakegen-geneva-transform.yaml`)

```yaml
rename-attrs:
  type: "urn:otel:processor:transform"
  config:
    kql_query: |
      logs | project-rename
        attributes["ThreadId"]   = attributes["OldThreadId"],
        attributes["ThreadName"] = attributes["OldThreadName"],
        attributes["CpuMs"]      = attributes["OldCpuMs"]
```

## 3. Restrict output to a column subset

**DE**

```xml
<DerivedEvent source="MAEvent" eventName="MAEventThin"
              storeType="Local" duration="PT1M">
  <Query>project TimeStamp, Level, Stream, Function, Message</Query>
</DerivedEvent>
```

**OTAP**

```yaml
thin-logs:
  type: "urn:otel:processor:transform"
  config:
    kql_query: |
      logs | project
        Timestamp,
        SeverityNumber,
        attributes["Stream"],
        attributes["Function"],
        Body
```

## 4. Drop a few columns (keep everything else)

**DE**

```xml
<DerivedEvent source="MyEvent" eventName="MyEventNoSecrets"
              storeType="Central" duration="PT1M">
  <Query>project-away Token, ConnectionString</Query>
</DerivedEvent>
```

**OTAP** — `attributes_processor` is the idiomatic choice, but transform also works

```yaml
no-secrets:
  type: "urn:otel:processor:transform"
  config:
    kql_query: |
      logs | project-away attributes["Token"], attributes["ConnectionString"]
```

`attributes_processor` form:

```yaml
no-secrets:
  type: "urn:otel:processor:attribute"
  config:
    actions:
      - { action: delete, key: "Token" }
      - { action: delete, key: "ConnectionString" }
```

## 5. Add a constant column

**DE**

```xml
<DerivedEvent source="MyEvent" eventName="MyEventTagged"
              storeType="Central" duration="PT1M">
  <Query>extend Pipeline = "geneva-mds-v2"</Query>
</DerivedEvent>
```

**OTAP**

```yaml
tag:
  type: "urn:otel:processor:transform"
  config:
    kql_query: |
      logs | extend attributes["Pipeline"] = "geneva-mds-v2"
```

`attributes_processor` form:

```yaml
tag:
  type: "urn:otel:processor:attribute"
  config:
    actions:
      - { action: upsert, key: "Pipeline", value: "geneva-mds-v2" }
```

## 6. Severity / level filter

**DE**

```xml
<DerivedEvent sourceRegex="UnstructuredLogEvent" eventName="topnerror"
              storeType="CentralBond" duration="PT1M">
  <Query><![CDATA[where Level < 3]]></Query>
</DerivedEvent>
```

**OTAP**

```yaml
errors-only:
  type: "urn:otel:processor:transform"
  config:
    kql_query: |
      logs | where SeverityNumber <= 9   # ERROR + FATAL in OTel
```

(`Level<3` in MA is roughly Critical/Error/Warning; pick the OTel `SeverityNumber` band that matches your mapping.)

## 7. Filter + project

**DE**

```xml
<DerivedEvent source="MAEvent" eventName="MAVerify"
              storeType="Local" duration="PT10S">
  <Query><![CDATA[
    where Level <= 3 and Stream != "Diag"
    | project Level, Stream, File, Function, Line, ErrorCode, Message
  ]]></Query>
</DerivedEvent>
```

**OTAP**

```yaml
ma-verify:
  type: "urn:otel:processor:transform"
  config:
    kql_query: |
      logs
      | where SeverityNumber <= 9 and attributes["Stream"] != "Diag"
      | project
          SeverityNumber,
          attributes["Stream"],
          attributes["File"],
          attributes["Function"],
          attributes["Line"],
          attributes["ErrorCode"],
          Body
```

## 8. Conditional rewrite of a value (`case`)

**DE**

```xml
<DerivedEvent source="ResourceLog" eventName="ResourceLogAnnotated"
              storeType="Central" duration="PT1M">
  <Query><![CDATA[
    extend Body = case(EventName == "azure.resource.log",
                       strcat(Body, "\n\nTroubleshooting..."),
                       Body)
  ]]></Query>
</DerivedEvent>
```

**OTAP** (this is the `fake-kql-debug-noop.yaml` example; works identically through `transform_processor`)

```yaml
annotate:
  type: "urn:otel:processor:transform"
  config:
    kql_query: |
      logs
      | extend Body = case(
          attributes["EventName"] == "azure.resource.log",
          strcat(Body, "\n\nTroubleshooting..."),
          Body)
```

## 9. Type / format coercion

**DE**

```xml
<DerivedEvent source="HttpEvent" eventName="HttpEventTyped"
              storeType="Central" duration="PT1M">
  <Query>extend StatusCode = tolong(StatusCode), Path = tolower(Path)</Query>
</DerivedEvent>
```

**OTAP**

```yaml
typify:
  type: "urn:otel:processor:transform"
  config:
    kql_query: |
      logs
      | extend
          attributes["http.status_code"] = tolong(attributes["http.status_code"]),
          attributes["http.target"]      = tolower(attributes["http.target"])
```

## 10. Compose / split string columns

**DE**

```xml
<DerivedEvent source="ServiceEvent" eventName="ServiceEventTagged"
              storeType="Central" duration="PT1M">
  <Query>extend ServiceFqdn = strcat(RoleInstance, ".", Datacenter, ".cloud")</Query>
</DerivedEvent>
```

**OTAP**

```yaml
fqdn:
  type: "urn:otel:processor:transform"
  config:
    kql_query: |
      logs
      | extend attributes["service.fqdn"] = strcat(
          attributes["RoleInstance"], ".",
          attributes["Datacenter"],   ".cloud")
```

## 11. Drop noisy events early

**DE**

```xml
<DerivedEvent source="DebugEvent" eventName="DebugEventFiltered"
              storeType="Local" duration="PT1M">
  <Query>where Level <= 4 and Component != "Heartbeat"</Query>
</DerivedEvent>
```

**OTAP** — when the predicate is just attribute equality, prefer `filter_processor`

```yaml
drop-noise:
  type: "urn:otel:processor:filter"
  config:
    logs:
      include:
        match_type: strict
        record_attributes:
          - { key: "Component", value: "Heartbeat" }   # excluded by routing
```

Or expressed with the transform processor:

```yaml
drop-noise:
  type: "urn:otel:processor:transform"
  config:
    kql_query: |
      logs | where attributes["Component"] != "Heartbeat" and SeverityNumber <= 13
```

## 12. PII redaction

**DE**

```xml
<DerivedEvent source="AccessEvent" eventName="AccessEventRedacted"
              storeType="Central" duration="PT1M">
  <Query>extend UserEmail = "[redacted]" | project-away ClientIp</Query>
</DerivedEvent>
```

**OTAP**

```yaml
redact:
  type: "urn:otel:processor:transform"
  config:
    kql_query: |
      logs
      | extend attributes["user.email"] = "[redacted]"
      | project-away attributes["client.ip"]
```

## 13. Two stages → either two nodes or one query

**DE** (two DEs forming a chain)

```xml
<DerivedEvent source="RawEvent" eventName="StagedEvent" storeType="Local" duration="PT1M">
  <Query>where Level <= 3</Query>
</DerivedEvent>
<DerivedEvent source="StagedEvent" eventName="StagedEventClean" storeType="Central" duration="PT1M">
  <Query>project-rename Msg = Message | project-away StackTrace</Query>
</DerivedEvent>
```

**OTAP option A — one query**

```yaml
clean-errors:
  type: "urn:otel:processor:transform"
  config:
    kql_query: |
      logs
      | where SeverityNumber <= 9
      | project-rename attributes["Msg"] = Body
      | project-away attributes["StackTrace"]
```

**OTAP option B — two nodes** (mirrors the DE chain)

```yaml
filter-errors:
  type: "urn:otel:processor:transform"
  config:
    kql_query: |
      logs | where SeverityNumber <= 9

reshape:
  type: "urn:otel:processor:transform"
  config:
    kql_query: |
      logs
      | project-rename attributes["Msg"] = Body
      | project-away attributes["StackTrace"]

connections:
  - { from: receiver,       to: filter-errors }
  - { from: filter-errors,  to: reshape }
  - { from: reshape,        to: exporter }
```

---

## What still does **not** map cleanly to `transform_processor`

If a DE relies on any of the following, you need either a different node or a redesign — `transform_processor` alone won't cover it:

- **`summarize` / `bin()` / time bucketing** (`duration="PT1M"` aggregations like `summarize Cnt=count() by bin(TimeStamp, 1m)`) → use `recordset_kql_processor` or `temporal_reaggregation_processor`.
- **`sourceRegex` fan‑in across many event names** → router/fanout topology.
- **`PostTaskActions` (signal event / queue notification)** → no equivalent.
- **`storeType` / table naming / `keepEventNameAsIs`** → choose the exporter; not a query concern.
- **Joins across two source events** → not supported in the DataFusion path.

For everything in examples 1‑13 above, `transform_processor` (often combined with `attributes_processor` or `filter_processor` for the trivial cases) is a clean drop‑in replacement for the DE.
