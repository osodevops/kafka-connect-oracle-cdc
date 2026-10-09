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
import java.util.List;
import java.util.regex.Pattern;
import sh.oso.connect.oracle.core.topology.CatalogSource;

/** Dictionary facts the rules need on top of {@link CatalogSource}; faked in unit tests. */
public interface DoctorCatalog extends CatalogSource {

  /** Tables matching the include patterns and not the exclude patterns, across the given PDBs. */
  List<CapturedTable> capturedTables(
      List<Pattern> include, List<Pattern> exclude, List<String> pdbs) throws SQLException;

  /** System privileges and roles granted (directly or through roles) to the connected user. */
  List<String> grantedPrivileges() throws SQLException;

  /** Fixed views the user cannot select from, out of the ones the engine needs. */
  List<String> inaccessibleViews() throws SQLException;

  /** The connected user's CONTAINER_DATA attribute covers all containers. */
  boolean containerDataAll() throws SQLException;

  /** Whether the connected user is a common user (CDB only). */
  boolean commonUser() throws SQLException;

  /**
   * V$ARCHIVED_LOG rows at the destination switched out at or after {@code since}, including
   * deleted entries, ordered by thread and sequence (DOC-9, DOC-10, sizing, redo-profile).
   */
  List<ArchiveStat> archiveHistory(Instant since, int destId) throws SQLException;

  /** V$LOG groups with their sizes (DOC-9, sizing). */
  List<OnlineLogGroup> onlineLogGroups() throws SQLException;

  /** The V$PARAMETER value of an initialisation parameter, or null when it is not visible. */
  String parameter(String name) throws SQLException;

  /**
   * Fixed tables with optimizer statistics (DOC-16), or -1 when DBA_TAB_STATISTICS is not readable.
   */
  int fixedTablesWithStatistics() throws SQLException;

  /**
   * Every pluggable database but the seed (DOC-21); empty on a non-CDB, null when {@code V$PDBS} or
   * the saved states are not readable.
   */
  java.util.List<PdbState> pdbStates() throws SQLException;

  /** Whether the connected user may execute the package {@code owner.name} (DOC-20). */
  boolean canExecute(String owner, String name) throws SQLException;

  /** Where the database runs (ADR-0024); fix text and DOC-23 depend on it. */
  sh.oso.connect.oracle.core.topology.Platform platform() throws SQLException;

  /**
   * An RDS configuration value from {@code rdsadmin.rds_configuration} (DOC-23), or null when the
   * user cannot read it or the database is not RDS.
   */
  String rdsConfiguration(String name) throws SQLException;

  /** The session's container id: 0 on a non-CDB, 1 at CDB$ROOT, above 2 inside a PDB. */
  int connectedContainerId() throws SQLException;
}
