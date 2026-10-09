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
package sh.oso.connect.oracle.e2e.support;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.topology.Platform;

/**
 * The database a qualification suite ({@code *QualIT}) runs against: the Oracle Database Free
 * container by default, or an external database (Amazon RDS, a standby, a RAC cluster) when {@code
 * -De2e.external.url} is set. Suites written against this interface use only what every target
 * allows: no SYSDBA, no container commands, no instance restarts.
 */
public interface TestDatabase {

  /** The external database when {@code e2e.external.url} is set, otherwise the container. */
  static TestDatabase get() {
    String url = System.getProperty("e2e.external.url");
    return url == null || url.isBlank()
        ? ContainerTestDatabase.get()
        : ExternalTestDatabase.fromSystemProperties();
  }

  /** The platform the target should be detected as. */
  Platform platform();

  /** A connection as the capture user, where mining happens. */
  Connection capture() throws SQLException;

  /** A connection as the owner of {@code schema}, for workload statements. */
  Connection workload(String schema) throws SQLException;

  /** Drops (if present) and creates {@code schema} with the grants tables need. */
  void recreateSchema(String schema) throws SQLException;

  void dropSchema(String schema) throws SQLException;

  /** The PDBs to capture: the container's FREEPDB1, or none on a non-CDB. */
  List<String> pdbs();

  /** An include pattern for one table of {@code schema}, with the PDB prefix where there is one. */
  String include(String schema, String table);

  /** Switches the current log and waits until it is archived. */
  void archiveLogCurrent() throws SQLException;

  /** The connector configuration for this database, as the doctor and the task read it. */
  CoreConfig coreConfig();

  /** ADR-0027: the connection is to a PDB as a local user, so the engine mines in range mode. */
  default boolean rangeMode() {
    return false;
  }

  /** What the evidence file records about the target: never a credential. */
  String describe();
}
