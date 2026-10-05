/*
 * Copyright 2026 OSO DevOps Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sh.oso.connect.oracle.core.testkit;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import sh.oso.connect.oracle.core.logs.RedoLog;
import sh.oso.connect.oracle.core.topology.ArchiveDestination;
import sh.oso.connect.oracle.core.topology.CatalogSource;
import sh.oso.connect.oracle.core.topology.DatabaseInfo;
import sh.oso.connect.oracle.core.topology.PdbInfo;
import sh.oso.connect.oracle.core.topology.ThreadInfo;

/** Scriptable catalog for unit tests of the inventory, topology and doctor rules. */
public final class FakeCatalog implements CatalogSource {

  public DatabaseInfo database =
      new DatabaseInfo(
          1234L,
          "FREE",
          true,
          "ARCHIVELOG",
          "READ WRITE",
          "PRIMARY",
          "23.0.0.0.0",
          1L,
          true,
          "Linux");
  public long currentScn = 1_000_000L;
  public final List<ThreadInfo> threads =
      new ArrayList<>(List.of(new ThreadInfo(1, true, "OPEN", 50)));
  public final List<PdbInfo> pdbs =
      new ArrayList<>(List.of(new PdbInfo(3, "FREEPDB1", "READ WRITE", 10L)));
  public final List<ArchiveDestination> destinations =
      new ArrayList<>(
          List.of(
              new ArchiveDestination(
                  1, "LOG_ARCHIVE_DEST_1", "/arch", "VALID", "LOCAL", "PRIMARY")));
  public final List<RedoLog> archived = new ArrayList<>();
  public final List<RedoLog> online = new ArrayList<>();

  /** Adds a contiguous run of archived logs for a thread, each spanning {@code span} SCNs. */
  public FakeCatalog archivedRun(
      int thread, long firstSequence, int count, long firstScn, long span) {
    long scn = firstScn;
    for (int i = 0; i < count; i++) {
      archived.add(
          new RedoLog(
              thread,
              firstSequence + i,
              scn,
              scn + span,
              "/arch/t" + thread + "_s" + (firstSequence + i) + ".arc",
              true,
              "A",
              false,
              1,
              false,
              false));
      scn += span;
    }
    return this;
  }

  public FakeCatalog onlineCurrent(int thread, long sequence, long firstScn) {
    online.add(
        new RedoLog(
            thread,
            sequence,
            firstScn,
            Long.MAX_VALUE,
            "/redo/t" + thread + "_g" + sequence + ".log",
            false,
            "CURRENT",
            false,
            0,
            false,
            false));
    return this;
  }

  public FakeCatalog markDeleted(int thread, long sequence) {
    archived.replaceAll(
        l ->
            l.thread() == thread && l.sequence() == sequence
                ? new RedoLog(
                    l.thread(),
                    l.sequence(),
                    l.firstScn(),
                    l.nextScn(),
                    l.path(),
                    true,
                    "D",
                    true,
                    l.destId(),
                    l.dictionaryBegin(),
                    l.dictionaryEnd())
                : l);
    return this;
  }

  /** A dictionary build beginning in log {@code begin} and complete in log {@code end}. */
  public FakeCatalog dictionaryBuild(int thread, long begin, long end) {
    archived.replaceAll(
        l ->
            l.thread() == thread && (l.sequence() == begin || l.sequence() == end)
                ? new RedoLog(
                    l.thread(),
                    l.sequence(),
                    l.firstScn(),
                    l.nextScn(),
                    l.path(),
                    l.archived(),
                    l.status(),
                    l.deleted(),
                    l.destId(),
                    l.dictionaryBegin() || l.sequence() == begin,
                    l.dictionaryEnd() || l.sequence() == end)
                : l);
    return this;
  }

  @Override
  public List<RedoLog> dictionaryLogs(int destId) {
    return archived.stream()
        .filter(l -> l.destId() == destId && (l.dictionaryBegin() || l.dictionaryEnd()))
        .toList();
  }

  public FakeCatalog removeArchived(int thread, long sequence) {
    archived.removeIf(l -> l.thread() == thread && l.sequence() == sequence);
    return this;
  }

  @Override
  public DatabaseInfo database() {
    return database;
  }

  @Override
  public long currentScn() {
    return currentScn;
  }

  @Override
  public List<ThreadInfo> threads() {
    return List.copyOf(threads);
  }

  @Override
  public List<PdbInfo> pdbs() {
    return List.copyOf(pdbs);
  }

  @Override
  public List<ArchiveDestination> archiveDestinations() {
    return List.copyOf(destinations);
  }

  @Override
  public List<RedoLog> onlineLogs() {
    return List.copyOf(online);
  }

  @Override
  public List<RedoLog> archivedLogs(long startScn, long endScn, int destId) {
    return archived.stream()
        .filter(l -> l.destId() == destId && l.overlaps(startScn, endScn))
        .toList();
  }

  @Override
  public List<RedoLog> archivedSince(Instant since, int destId) {
    return archived.stream().filter(l -> l.destId() == destId).toList();
  }
}
