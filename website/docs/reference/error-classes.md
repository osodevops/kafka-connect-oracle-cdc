---
title: Error classes
description: The CDC-nnnn error codes, what each means, whether the engine retries and where the runbook is.
---

# Error classes

Every stop has a code, an operator action and a runbook. Codes are stable and tests reference
them. `CDC-1xxx` conditions are retried within the retry budget; everything else stops the task.

| Code | Name | Meaning | Runbook |
|---|---|---|---|
| CDC-1001 | TRANSIENT_DATABASE | The database or network dropped the connection; retried with backoff | [transient-database](operations/runbooks/transient-database.md) |
| CDC-1002 | MINING_STEP_RETRY | A LogMiner step failed in a way that re-mining the same range fixes | [mining-step-retry](operations/runbooks/mining-step-retry.md) |
| CDC-2001 | LOG_GAP | A redo thread has a sequence gap in the archived logs | [log-gap](operations/runbooks/log-gap.md) |
| CDC-2002 | LOG_PURGED | A needed archived log was deleted | [log-purged](operations/runbooks/log-purged.md) |
| CDC-3001 | DECODE | A redo statement for a captured table could not be decoded | [decode](operations/runbooks/decode.md) |
| CDC-3002 | CORRUPTION | Redo or journal content failed an integrity check | [corruption](operations/runbooks/corruption.md) |
| CDC-4001 | BUFFER_EXHAUSTED | The transaction buffer exceeded memory and spill limits | [buffer-exhausted](operations/runbooks/buffer-exhausted.md) |
| CDC-4002 | JOURNAL_CORRUPTION | The transaction journal topic failed to load | [journal-corruption](operations/runbooks/journal-corruption.md) |
| CDC-4003 | MINING_STALLED | Mining steps kept timing out after halving | [mining-stalled](operations/runbooks/mining-stalled.md) |
| CDC-5001 | TOPOLOGY | The database identity, role or thread set changed unexpectedly | [topology](operations/runbooks/topology.md) |
| CDC-5002 | PRIVILEGE | The mining user lacks a grant | [privilege](operations/runbooks/privilege.md) |
| CDC-6001 | DICTIONARY_UNAVAILABLE | Redo predates a schema change and no redo dictionary covers it | [dictionary-unavailable](operations/runbooks/dictionary-unavailable.md) |
| CDC-6002 | UNSUPPORTED_DDL | A DDL on a captured table is not supported | [unsupported-ddl](operations/runbooks/unsupported-ddl.md) |
| CDC-7001 | ORPHAN_RELEASE_VIOLATION | A transaction released as orphaned later committed | [orphan-release-violation](operations/runbooks/orphan-release-violation.md) |
