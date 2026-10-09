# ADR-0026: The position is a vector of per-thread redo addresses; commit order is per thread

**Status:** Accepted. In progress: the step cursor (3a) is implemented. Still to come: the
per-thread position and resume rule (3b), the inventory and safety rules for closed and disabled
threads (3c), and lifting the RAC refusal of ADR-0023 with the RAC regression corpus and a RAC lab
run (3d, blocked on a RAC lab).
**Context:** PRD-00 CORE-POS-1, CORE-POS-3, CORE-POS-4, CORE-MINE-4 and CORE-LOG-3; ADR-0014 (the
redo byte address cursor); ADR-0023 (a second redo thread is refused).

## Context

The redo byte address cursor of ADR-0014 orders rows by RS_ID, then SSN. RS_ID begins with the log
sequence of the thread that wrote the record, and each RAC instance writes its own thread with its
own sequence. Two threads therefore produce overlapping RS_ID values, and one thread being further
on in its redo says nothing about another. With a single cursor, a slow thread's later rows sort
below the faster thread's mark and are skipped without a trace; this is why ADR-0023 refuses a
second enabled thread.

LogMiner merges the threads' redo by SCN but keeps redo order within a thread, and ADR-0014 showed
that redo order and SCN order disagree within a thread (late-bound private strands). No total order
over `(commit SCN, thread, XID)` matches the order in which the connector emits commits. What is
exact is that the commits acknowledged on each thread are a prefix of that thread's redo order.

## Decision

1. `(THREAD#, RS_ID, SSN)` names a redo record. A redo byte address is compared only with another
   of the same thread; no code compares addresses across threads.
2. The step cursor holds one mark per thread. Each step's query bounds every marked thread by its
   own address and reads a thread without a mark by SCN from the step's start. A cursor resumed from
   a position that names no thread keeps one thread-less mark, which is exact while one thread is
   mined, and today's query unchanged; the first row applied on a real thread replaces it.
3. The position records, per thread, the resume address and the last acknowledged commit, in a
   `threads` block of the existing `v=1` offset; the legacy keys stay authoritative for older
   readers, and a single-thread position writes no `threads` block, so its offset is unchanged.
   (3b)
4. A commit arriving below the acknowledged resume floor of its thread stops the task with
   CDC-7003 instead of being dropped. (3b, 3c)

## Consequences

- Single-instance capture is unchanged: same offsets, same resume points, and after the first
  step a query with one `THREAD# = n` term.
- RAC capture remains refused (ADR-0023) until 3d.

## Evidence

`StepCursorTest`, `LogMinerQueryTest` (per-thread form; the thread-less form is today's SQL),
`StepRunnerTest.aSlowThreadsRowsAreNotHiddenByAnotherThreadsAddress` (with a single cursor the
slow thread's rows are skipped; with a mark per thread they are mined).

## PRD edits

To follow with 3b: PRD-00 CORE-POS-1, CORE-POS-3, CORE-POS-4 and CORE-MINE-4.

## Amendments

None.
