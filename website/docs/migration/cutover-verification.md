---
title: Cutover verification
description: "How a migration is proven: state equivalence at a check SCN, committed transaction set and event invariants."
---

# Cutover verification

**Status:** Phase 1b and 1d. The verifier applies three checks: table state equivalence at a
quiesced SCN against `SELECT ... AS OF SCN`, equality of the committed transaction id set with
the ledger or the previous connector's output, and event-level invariants (no rolled-back rows,
no uncommitted rows, monotonic commit order). An ORA-01555 during the check marks the run
inconclusive rather than failed. It writes an evidence JSON with a SHA-256 you can attach to a
change record.
