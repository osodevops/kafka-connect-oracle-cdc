# ADR-0026: The position is a vector of per-thread redo addresses; commit order is per thread

**Status:** Accepted. In progress: the step cursor (3a), the per-thread position, skip rule and
resume vector (3b), and the thread states, per-thread windows, safety lag and CDC-7003 stop (3c)
are implemented. Still to come: lifting the RAC refusal of ADR-0023 with the RAC regression corpus
and a RAC lab run (3d, blocked on a RAC lab).
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
4. A row or commit that a restart from a position already handed to the sink would not deliver
   stops the task with CDC-7003 (`ResumeFloorViolationException`) instead of being dropped by
   that restart. The floor is kept per thread, in redo byte addresses: a row of thread t below the
   highest resume address handed for t (a restart reads t again from there, inclusive), or a
   commit of t emitted at or before the last commit handed for t (the skip rule skips it). The
   start position is the first floor; its own last commit may be emitted again from its event
   index. Rows without an address are not compared. (3c)
5. Thread states (CORE-LOG-3), with more than one thread in `V$THREAD`: a CLOSED thread whose last
   log (`V$THREAD.SEQUENCE#`) is archived is closed at that log's `NEXT_CHANGE#`: required up to
   there, then no longer waited for by the archive-only safe end or the online step end. A
   DISABLED thread's logs in the range are mined and it is never waited for. A thread that opens
   again, or is enabled for the first time, is required from its first new log; an SCN gap before
   that log is no gap when the log of the previous sequence ends at or before the range, or the log
   is sequence 1, because sequences have no holes. When every enabled thread is closed, and on a
   database with one thread, the rules of before apply. A change of a thread's state is an ops
   event (`thread-state-changed`). (3c)
6. The scheduler counts log boundaries per thread: a window of N logs ends at the earliest N-th
   boundary over the threads, so no thread contributes more than N logs and the thread that
   switches most contributes exactly N. A thread with fewer than N boundaries does not lengthen a
   step, so timeouts still shrink every thread to one log. (3c)
7. `cdc.rac.safety.lag.ms` is read: with more than one enabled thread at the start, the online safe
   end is the newest (clock, current SCN) read at least the lag old (-1 means 3000 ms); with one
   thread it is 0 whatever the key says, so the safe end stays the current SCN. Defence in depth: the
   per-thread cursor is what mines late redo. (3c)

### Why CDC-7003 compares redo addresses, not SCNs

The plan first defined the violation as a commit SCN below the floor of the last position handed
to the sink. That is wrong on a single instance: a late-bound private strand gives a commit an SCN
below records already mined (ADR-0014), the redo byte address cursor mines it after them, and a
restart from the earlier position mines it again because its address is after that position's. An
SCN comparison would stop healthy capture there, and on RAC every slow instance's commit would
trip it. What a restart actually loses is what lies before a thread's resume address, or at or
before its last acknowledged commit, in that thread's redo order, so that is the floor.

Each thread's rows reach the engine in redo order after the step cursor's mark (the step runner
already drops a row at or before the mark), and every resume address handed over is at or below
what was applied. The condition can therefore only arise from LogMiner returning a thread out of
redo order within a step or from a bug in the resume calculation: CDC-7003 is a defensive stop,
not a case expected in operation.

One case reaches the floor without a loss: a reconnect after a session dies part way through
applying a step. Commits already emitted from that step raised the floor, the cursor did not
advance, and the step is mined again, so its earlier rows come back below the floor; they are
re-applied and delivered again, a duplicate. `ReconnectEngineIT` met this on Oracle Database Free
(12 October 2026). The check therefore runs only when more than one redo thread is qualified;
whether it needs to tell such a re-mined step apart on RAC is settled with the RAC lab (3d).

## Consequences

- Single-instance capture is unchanged: same offsets, same resume points, and after the first
  step a query with one `THREAD# = n` term. The thread-state rules, the per-thread window and the
  safety lag do nothing with one thread (3c), and the CDC-7003 check is not run with one thread.
- RAC capture remains refused (ADR-0023) until 3d.
- The inventory asks `V$ARCHIVED_LOG` for one log by thread and sequence only with more than one
  thread: for a CLOSED thread's last log, and when a gap would otherwise be raised.
- A thread with no applied row is still read by SCN from the step's start (decision 2). A
  late-bound first transaction of an idle thread, written after a step passed its SCNs, would not
  be returned; the safety lag narrows that window but does not close it. To settle before RAC is
  qualified (3d).

## Evidence

`StepCursorTest`, `LogMinerQueryTest` (per-thread form; the thread-less form is today's SQL),
`StepRunnerTest.aSlowThreadsRowsAreNotHiddenByAnotherThreadsAddress` (with a single cursor the
slow thread's rows are skipped; with a mark per thread they are mined). 3b: `PositionCodecTest`
(the `threads` block and its agreement with the legacy keys), `ResumeAndSkipTest` (skip rule per
thread), `PerThreadPositionTest` (one thread writes the offset of before, byte for byte),
`ResumePointTest`, `RecordQueueSinkTest`, and `ResumesEachThreadFromItsOwnPositionTest` (tag
`rac-per-thread-restart`: a restart from any acknowledged record of two interleaved threads delivers
every change exactly once; with one resume address it loses the quiet thread's commits). 3c:
`MinesTheClosedThreadsTailAfterAnInstanceCrashTest` (`rac-instance-crash`; before the change the
surviving thread's commits after the crash were never mined), `HandlesAThreadDisabledMidRunTest`
(`rac-thread-disabled`; before, CDC-2001 when the thread came back), `StopsWhenACommitArrivesBelowTheAcknowledgedFloorTest`
(`rac-resume-floor`; before, the commit was emitted and a restart from the previous offset lost it),
`MinesALateCommitOfASlowThreadTest` (`rac-late-commit`), `CaptureEngineTest`
`aLateBoundCommitOnOneThreadIsDeliveredAndNeverAResumeFloorViolation`, `LogInventoryTest`,
`MiningSchedulerTest` (`rac-window`), `SafetyLagTest`, `ResumeFloorTest`.

## PRD edits

PRD-00 CORE-POS-1, CORE-POS-3, CORE-POS-4 and CORE-MINE-4 (with 3b). 3c: CORE-LOG-3, CORE-MINE-4
(per-thread window), the `cdc.rac.safety.lag.ms` row and the CORE-ERR table.

## Amendments

None.
