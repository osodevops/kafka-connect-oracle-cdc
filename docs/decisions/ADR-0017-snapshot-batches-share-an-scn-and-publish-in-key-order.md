# ADR-0017: Snapshot batches share one SCN and publish in key order

**Status:** Accepted
**Context:** P1-19 (PRD-02 sections 3 and 4, ADR-0004), 5 October 2026.

## Context

PRD-02 reads each chunk as of its own SCN and records progress as completed chunk ranges compacted
into a frontier plus an exception list (SNAP-3). With chunks of one table read in parallel, each at
its own SCN, a later chunk can finish and become publishable before an earlier one. Keeping
exceptions in the offset then needs either the whole chunk plan in the offset (too large to carry on
every record) or an ordering of key values in Java that matches the database's (fragile across
character sets). PRD-02 also assumes a chunk's rows are held until streaming passes its SCN, but
does not say what happens to commits mined while the chunk is still being read.

## Decision

1. **Batches.** A table is read in batches of up to `cdc.snapshot.threads` contiguous chunks, all
   read as of one SCN taken just before the batch, in parallel on reader connections. Batches of a
   table are published in key order. A table's progress is one frontier, the lower bound of its next
   chunk, so the offset's snapshot block stays a few bytes per table, and no key ordering is
   computed outside the database.
2. **Planning.** Chunk bounds are found from the frontier with `ORDER BY <key> OFFSET n ROWS FETCH
   NEXT 1 ROWS ONLY`, one bound at a time, instead of `NTILE` over the whole table: no full sort up
   front, and a table that grows is still covered, because the last chunk is open-ended. Composite
   keys use a lexicographic predicate written out column by column. Keys qualify only when every
   column is NOT NULL and of a range type; otherwise heap tables use ROWID ranges from their
   extents, and other tables one chunk. Sessions sort and compare in binary.
3. **Streaming waits for a batch in flight.** From the moment a batch's SCN is taken until the batch
   is read, the sink does not publish a commit at or after that SCN; it waits. Otherwise a commit
   could precede an older image of its row. The wait lasts one chunk read.
4. **Values.** Temporal and interval columns are selected as text in the mining session's formats
   and decoded by the same codec as redo literals, so a snapshot record and a change record of one
   row carry identical values (`TypeRoundTripEngineIT` checks it).
5. **Retries.** A failed chunk keeps the chunks before it in its batch and is read again with a
   fresh SCN (SNAP-3). ORA-01555 and ORA-08181 also halve the chunk size, down to 1,000 rows, then
   stop with `SnapshotTooOldException` (`CDC-8001`, SNAP-4). ORA-01466 waits for the SCN-to-time
   mapping, then retries.
6. **When a snapshot starts.** `initial` and `snapshot_only` start one only when there is no stored
   offset; the first offset already carries the snapshot block, so a crash before the first chunk
   still resumes it. A stored unfinished block resumes in every mode.

## Consequences

- Up to one batch per table is read again after a crash, not only one chunk.
- Streaming pauses for the length of a batch read whenever it reaches a batch's SCN.
- Deferred: snapshots by signal (P1-20), resnapshot by command (P1-24), partition-by-partition
  reads (SNAP-10), restarting a table whose key changes (SNAP-9), snapshots of tables added later
  (P1-22), spilling held chunks to disk, and reads from a standby (SNAP-11, Phase 2).

## Evidence

`SnapshotChunksEngineIT` (composite key, keyless heap by ROWID, index-organised table: every row
read once), `TypeRoundTripEngineIT` (value parity), `SnapshotConnectorIT` (correctness oracle under
concurrent writes; a stop part way resumes at the frontier), `SnapshotCoordinatorTest` and the task
tests.

## PRD edits

PRD-02 SNAP-3: progress is a frontier per table (ADR-0017), not a frontier plus exceptions.
