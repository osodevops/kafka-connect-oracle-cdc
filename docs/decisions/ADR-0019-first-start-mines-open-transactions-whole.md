# ADR-0019: A first start mines the transactions already open whole

**Status:** Accepted
**Context:** PRD-02 section 3 (first deployment), PRD-00 CORE-POS, regression
[dbz#2779](https://github.com/debezium/dbz/issues/2779), 6 October 2026.

## Context

PRD-02 section 3 says a first deployment records `start_scn = CURRENT_SCN` and starts mining from
it. A transaction that is already open at that SCN has changes in the redo before it. Mining from
`start_scn` returns only its later changes and its commit, so the connector published a partial
transaction: the earlier changes were lost, with or without an initial snapshot (a chunk read at an
SCN before the commit does not see uncommitted rows either). The regression corpus reproduced it
(`KeepsTransactionsOpenAtSnapshotStartAcrossRestartConnectorIT`). The same holds for `cdc.start.scn`
when a transaction open at that SCN is still open when the connector starts.

Mining from an earlier SCN alone is not enough. The rows of transactions that ended before the
start would then be read as well: their commits must not be published, and decoding their rows
could stop the task for a change it is about to skip (a type the decoder cannot read, or generic
`COL n` names when a DDL followed them, which would start the lag case replay of ADR-0016).

## Decision

1. **Three reads at a first start.** With no stored offset the task reads the current SCN `q`,
   then the transactions `GV$TRANSACTION` lists in the captured containers (joined to
   `V$CONTAINERS` on a container database), then the start SCN `s`: the current SCN again, or
   `cdc.start.scn` (then `q` is `min(q, s)`). Every transaction open at `s` either is in the list
   (it began before `q` and was still open when the list was read) or began at or after `q`.
2. **Mining begins at the earlier of `q` and the oldest listed start SCN.** When that is `s`
   itself, the position is the plain first start of PRD-02.
3. **Below `s`, only those transactions are kept.** Before a step is decoded, its events below
   `s` are dropped unless their transaction is listed or its START row is at or after `q`. DDL
   below `s` is dropped too: the dictionary the task reads at start already reflects it. Commits
   below `s` are skipped whole (`SkipRule`), which covers a listed transaction that committed
   between the list and `s`.
4. **The rule lives in the offset until the first acknowledged commit.** `s`, `q` and the listed
   transactions are kept in the position's extras (`start_floor_scn`, `start_open_scn`,
   `start_open_xids`), so a restart before any commit is acknowledged applies the same rule. A
   position with an acknowledged commit drops them: every later commit is above `s`.
5. **Missing redo is a stop.** If the redo from the oldest open transaction's start is no longer
   available, the task stops with `CDC-2002` like any other gap. The operator waits for the
   transaction to end, or ends it; no setting skips it.

## Consequences

- A first start reads redo from the start of the oldest open transaction in the captured
  containers, which may be long before the current SCN when a long transaction is open. The log
  line at start names that transaction.
- Transactions of excluded users and transactions that never touch a captured table still move
  the mining start back, because `GV$TRANSACTION` cannot tell which tables a transaction touched.
  Their rows are filtered as usual once mined.
- For `cdc.start.scn`, a transaction that was open at that SCN and has since committed cannot be
  seen in `GV$TRANSACTION`; its rows before the SCN are not read. `takeover_scn.py` sets the SCN
  from Debezium's offset `scn`, which is at or before the start of every transaction Debezium had
  open, so a takeover is not affected.
- Real Application Clusters (Phase 2) need the per-thread position vector; the rule here is per
  database.

## Evidence

- `MinesTransactionsOpenAtTheFirstStartWholeTest` (T0, `dbz-2779`): an open transaction is
  delivered whole, a transaction that ended before the start is not decoded, the old start loses
  the early changes, and a restart before the first commit applies the same rule.
- `KeepsTransactionsOpenAtSnapshotStartAcrossRestartConnectorIT` (T1 connector, `dbz-2779`): the
  same against Oracle Database Free with an initial snapshot and a restart part way through it.

## PRD edits

PRD-02 section 3, the first-deployment paragraph: the connector records the start SCN, mines from
the start of the oldest transaction open at it, and publishes only the commits from the start SCN
on, with every open transaction whole (ADR-0019).
