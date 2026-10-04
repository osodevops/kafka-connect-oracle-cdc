# Security policy

## Supported versions

The current and the previous minor release receive security fixes, backported to both.

## Reporting a vulnerability

Use GitHub private vulnerability reporting on this repository, or email security@oso.sh. Do not
open a public issue.

## Targets

- Acknowledgement within two business days.
- Severity assessment within five business days.
- Critical and high severity issues fixed or mitigated within ten business days.
- Coordinated disclosure, up to 90 days. Reporters are credited unless they prefer not to be.

## Publication

A GitHub Security Advisory (with a CVE where applicable), a patch release for every supported
minor with rebuilt plugin archives, and direct notice to enterprise support subscribers.

## Scope notes

Database credentials are Connect `PASSWORD` configs and are never logged. Any path that could
leak credentials or row data into logs, ops events or reports is treated as high severity. Row
values appear in logs only when `cdc.log.sensitive.data=true`. Dependencies are monitored by
Dependabot and the build's licence and vulnerability checks.
