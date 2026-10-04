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
package sh.oso.connect.oracle.core.doctor;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import sh.oso.connect.oracle.core.logs.RedoLog;
import sh.oso.connect.oracle.core.testkit.FakeCatalog;
import sh.oso.connect.oracle.core.topology.ArchiveDestination;
import sh.oso.connect.oracle.core.topology.DatabaseInfo;
import sh.oso.connect.oracle.core.topology.PdbInfo;
import sh.oso.connect.oracle.core.topology.ThreadInfo;

/** A {@link FakeCatalog} plus the dictionary facts the doctor rules need. */
final class FakeDoctorCatalog implements DoctorCatalog {
  final FakeCatalog base = new FakeCatalog();
  final List<CapturedTable> tables = new ArrayList<>();
  final List<String> privileges = new ArrayList<>(Rules.REQUIRED_PRIVILEGES);
  final List<String> inaccessible = new ArrayList<>();
  boolean containerDataAll = true;
  boolean common = true;

  @Override
  public List<CapturedTable> capturedTables(
      List<Pattern> include, List<Pattern> exclude, List<String> pdbs) {
    return tables.stream()
        .filter(
            t ->
                include.stream().anyMatch(p -> p.matcher(t.fqn()).matches())
                    && exclude.stream().noneMatch(p -> p.matcher(t.fqn()).matches()))
        .toList();
  }

  @Override
  public List<String> grantedPrivileges() {
    return privileges;
  }

  @Override
  public List<String> inaccessibleViews() {
    return inaccessible;
  }

  @Override
  public boolean containerDataAll() {
    return containerDataAll;
  }

  @Override
  public boolean commonUser() {
    return common;
  }

  @Override
  public DatabaseInfo database() throws SQLException {
    return base.database();
  }

  @Override
  public long currentScn() throws SQLException {
    return base.currentScn();
  }

  @Override
  public List<ThreadInfo> threads() throws SQLException {
    return base.threads();
  }

  @Override
  public List<PdbInfo> pdbs() throws SQLException {
    return base.pdbs();
  }

  @Override
  public List<ArchiveDestination> archiveDestinations() throws SQLException {
    return base.archiveDestinations();
  }

  @Override
  public List<RedoLog> onlineLogs() throws SQLException {
    return base.onlineLogs();
  }

  @Override
  public List<RedoLog> archivedLogs(long startScn, long endScn, int destId) throws SQLException {
    return base.archivedLogs(startScn, endScn, destId);
  }

  @Override
  public List<RedoLog> archivedSince(Instant since, int destId) throws SQLException {
    return base.archivedSince(since, destId);
  }
}
