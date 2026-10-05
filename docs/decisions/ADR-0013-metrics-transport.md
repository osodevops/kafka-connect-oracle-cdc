# ADR-0013: Metrics transport

**Status:** Accepted (design)
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

Plain JMX MBeans plus a shipped JMX Prometheus exporter configuration (`ops/jmx-exporter/oracle-cdc.yml`); the Micrometer bridge named in the feasibility report is deferred to keep the plugin's dependency set small. Implemented in Phase 1b (P1-23).

## Amendment, 5 October 2026 (P1-23)

Implemented as one MXBean per task, `sh.oso.cdc:type=task,server=<cdc.topic.prefix>`
(`core/metrics/TaskMetricsMXBean`). Each getter carries a `@Description` with its kind (counter or
gauge); `MetricsReferenceTest` generates website/docs/reference/metrics.md and the exporter rules in
`ops/jmx-exporter/oracle-cdc.yml` from them, and fails when the dashboard or the alert rules name a
metric the rules do not export. Prometheus names are `oracle_cdc_<attribute in snake case>`, with
`_total` on counters so the names hold across exporter versions. The buffer belongs to the engine
thread, so the engine publishes an immutable snapshot (buffer metrics and the 20 largest open
transactions, CORE-TX-8) after every step and idle poll; JMX readers see only that snapshot and the
atomic counters. Registration failures are logged and never stop the task.

PRD-00 CORE-MINE-6 (pipelining) is implemented as parallel decoding of a step after it is read: rows
up to the step's first DDL are decoded on `cdc.mining.decode.threads` threads (a fork-join pool,
schema lookups on the engine thread) when the step has at least 256 rows, and applied in redo order
by the engine thread as before. Fetch and decode do not overlap yet; that is a later optimisation and
needs no change to the ordering or error semantics proven by `ParallelDecodeEngineTest` and
`MetricsEngineIT`.

