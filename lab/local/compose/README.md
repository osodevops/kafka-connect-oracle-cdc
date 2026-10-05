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
