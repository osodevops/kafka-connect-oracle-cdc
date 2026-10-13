# 0.1.6 preview release decision

On 13 October 2026 Sion Smith authorised publishing the preview without waiting for the outstanding
long T2 qualification. This is an explicit exception for 0.1.6, not a change to future release gates.
The recorded decision is [release/overrides/0.1.6.json](../../../release/overrides/0.1.6.json).

The full T2 matrix is **not completed**. The hosted nightly was cancelled, and the replacement
local run was stopped following the maintainer's decision. Neither is represented as passing
qualification. The older failing worker-kill regression and its subsequent header fix remain
recorded in the handover; all regression assertions remain in the repository.

Existing [green CI](https://github.com/osodevops/kafka-connect-oracle-cdc/actions/runs/38425197078)
at `5b6f8cd` covers Java 17 and 21, unit and quality gates, both integration tiers, migration tools,
secret scanning and the Central dry run. Release automation verifies that capture sources, tests,
dependencies and database-image inputs are identical to that baseline, normalising only the
project's release version. These checks are reused for this preview rather than repeated.

The [format qualification](../2026-10-13-confluent-format/README.md), external-output reference,
live comparison and same-topic Avro cutover are already complete. The provenance notice and all
documented differences remain unchanged, including typed refusal of Decimal rounding in snapshots
and streaming. The licensing approval remains the recorded maintainer decision, not a solicitor's
opinion or vendor permission.

Release version validation, signing, Central publication, artefact checksums, SBOM/provenance and
the release environment approval remain in place. A full 72-hour soak, full platform matrix,
live non-CDB format parity and XMLTYPE LOB representation are not claimed as complete.

## Publication

PR #29 merged at `a7489b9`. Signed Central publication passed in
[run 38447716371](https://github.com/osodevops/kafka-connect-oracle-cdc/actions/runs/38447716371).
All four module/parent POMs are available on Maven Central. The GitHub plugin ZIP, CLI, SBOM and
checksums were attached by asset recovery
[run 38455317985](https://github.com/osodevops/kafka-connect-oracle-cdc/actions/runs/38455317985),
using the original signed publication's release files without redeploying Central.
