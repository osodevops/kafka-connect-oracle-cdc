# lab/local/compose: Oracle Database Free, Kafka and Kafka Connect on Docker

```bash
make -C lab/local/compose up          # package the plugin, copy it into plugins/, start the stack and wait for health
make -C lab/local/compose register    # register examples/connector-freepdb1.json
make -C lab/local/compose status      # connector and task state
make -C lab/local/compose topics      # kcat topic list
make -C lab/local/compose down
```

The Connect worker loads the Prometheus JMX exporter agent (downloaded by `make up`) with the rules in
`ops/jmx-exporter`, so the task metrics are on port 9404.

Profiles: `PROFILES="--profile observability"` adds Prometheus and Grafana wired to `ops/`;
`--profile chaos` adds Toxiproxy between Connect and Oracle (point the connector at
`toxiproxy:11521`); `--profile debezium` runs a Debezium 3.7 Connect for internal comparison only.

Verified 4 October 2026 on an Apple Silicon workstation: all three services healthy, the plugin
listed once by `GET /connector-plugins`, the example connector and its task RUNNING.

## Soak (testing strategy T3)

`make soak` runs `bench soak` against the registered connector: the paced workload of
`bench/src/main/resources/soak/default.json` (two sessions, 50 ms between transactions, no LOBs
because the example connector skips them) for `HOURS`, the correctness oracle every `CHECK_EVERY`
hours and once more after the workload stops. The oracle reads the table topics from the
beginning, so the soak refuses to start when they already hold records: begin from a fresh stack.

```bash
make -C lab/local/compose down up register soak HOURS=1 CHECK_EVERY=0.25   # one-hour trial
make -C lab/local/compose down up register soak                           # 72 hours, a check every 6
```

At each check point the workload pauses until every session is parked, the soak reads the
database SCN and waits until the connector's committed `resume_scn` is past it (the catch-up time,
which includes up to one heartbeat and one offset flush), then runs the oracle and resumes. The
first FAIL stops the soak with its evidence; an INCONCLUSIVE check is recorded and the soak goes
on; a catch-up longer than 30 minutes stops it as LAG, which is a finding about lag rather than a
correctness verdict. A failed task stops it at once. Exit codes: 0 pass, 1 fail, 3 inconclusive,
4 lag, 5 error.

Everything lands in `lab/local/compose/soak-out/<start time>/`: `check-001.json` onwards (the
oracle's evidence), `metrics.csv` (one row a minute: worker heap, lag, queue depth and buffer
gauges from the JMX exporter on port 9404, and the workload counters), `soak.log`, `summary.json`
and `summary.md`. The figures are internal: never publish them (Oracle's development licence terms
forbid publishing database benchmark results).

Before a long run:

- Keep the Mac awake for the whole run, on mains power with the lid open. A sleeping host freezes
  Docker and the soak alike; overnight it has slept in cycles even with a `caffeinate` idle
  assertion. The summary counts clock jumps of more than a minute as suspected host sleep; check
  `pmset -g log | grep -E "Sleep|Wake"` as well before trusting a slow run.
- The oracle holds every transaction of the run in memory at each check, so the soak's heap grows
  with the run. `SOAK_HEAP` sets it (default `12g`); `harnessHeapPeakBytes` in `summary.json`
  shows each check's peak, so a one-hour trial tells you what the full run needs.
- Kafka keeps every record and Oracle keeps every archived log under `/opt/oracle/archive`; make
  sure Docker Desktop has the disk space for the whole run.
