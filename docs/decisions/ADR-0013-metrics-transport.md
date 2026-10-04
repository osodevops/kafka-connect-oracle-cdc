# ADR-0013: Metrics transport

**Status:** Accepted (design)
**Context:** see `docs/00_feasibility_and_strategy.md` and the PRD it amends; recorded during the implementation plan of 4 October 2026.

## Decision

Plain JMX MBeans plus a shipped JMX Prometheus exporter configuration (`ops/jmx-exporter/oracle-cdc.yml`); the Micrometer bridge named in the feasibility report is deferred to keep the plugin's dependency set small. Implemented in Phase 1b (P1-23).
