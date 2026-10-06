# docker/test-oracle: derived Oracle Database Free test image

Base: `gvenzl/oracle-free:23.26.3-slim-faststart` (multi-arch, amd64 and arm64). The first start
runs the hooks in `initdb.d/` once as SYS: archive destination, ARCHIVELOG mode, minimal
supplemental logging, three 50 MB redo log groups, the `C##CDC` common capture user and a
`WORKLOAD` schema in `FREEPDB1`, `FREEPDB2` and `FREEPDB3` (created by `initdb.d/03a-third-pdb.sql`). `startdb.d/00-assert.sh` runs on every start and
stops the container if any of that is missing.

```bash
make build          # BASE_TAG=23.9-faststart make build for the second matrix version
make run            # ORACLE_PASSWORD=oracle, port 1521
make logs           # wait for "DATABASE IS READY TO USE!" after the hooks
make status
```

The Testcontainers harness in `e2e-tests` builds this image itself (`ImageFromDockerfile`) and
tags it by the SHA-256 of this directory, so the image is rebuilt only when these files change.

## Licence basis

The base image is pulled by the person running the build, who accepts Oracle's terms for Oracle
Database Free. This derived image is used for development and testing only and is never
published outside OSO's own registry. Lab databases hold synthetic data only. Measured start-up
times are recorded in CI logs, not here (Oracle's development licence forbids publishing
benchmark results).

## Measured first-start overhead

On an Apple Silicon workstation with the base image already pulled, the first start (PDB
creation, ARCHIVELOG conversion with its restart, redo log resize, user creation) reports
`DATABASE IS READY TO USE!` well under a minute; a GitHub-hosted amd64 runner is expected to take
one to three minutes including the base image pull. These are ranges for planning, not benchmark
results. Re-measure in CI logs when the base tag changes.
