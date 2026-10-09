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
import java.util.Map;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.topology.Platform;

/** {@link TestDatabase} over the shared Oracle Database Free container, mining FREEPDB1. */
final class ContainerTestDatabase implements TestDatabase {

  private static volatile ContainerTestDatabase instance;

  private final OracleTestDatabase db;

  private ContainerTestDatabase(OracleTestDatabase db) {
    this.db = db;
  }

  static ContainerTestDatabase get() {
    if (instance == null) {
      synchronized (ContainerTestDatabase.class) {
        if (instance == null) {
          instance = new ContainerTestDatabase(OracleTestDatabase.get());
        }
      }
    }
    return instance;
  }

  @Override
  public Platform platform() {
    return Platform.ONPREM;
  }

  @Override
  public Connection capture() throws SQLException {
    return db.capture(OracleTestDatabase.CDB_SERVICE);
  }

  @Override
  public Connection workload(String schema) throws SQLException {
    return db.connect(OracleTestDatabase.PDB1, schema, schema);
  }

  @Override
  public void recreateSchema(String schema) throws SQLException {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
  }

  @Override
  public void dropSchema(String schema) throws SQLException {
    SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
  }

  @Override
  public List<String> pdbs() {
    return List.of(OracleTestDatabase.PDB1);
  }

  @Override
  public String include(String schema, String table) {
    return OracleTestDatabase.PDB1 + "\\." + schema + "\\." + table;
  }

  @Override
  public void archiveLogCurrent() throws SQLException {
    OracleSql.archiveLogCurrent(db);
  }

  @Override
  public CoreConfig coreConfig() {
    return new CoreConfig(
        Map.of(
            CoreConfig.DATABASE_HOST,
            db.container().getHost(),
            CoreConfig.DATABASE_PORT,
            String.valueOf(db.container().getMappedPort(1521)),
            CoreConfig.DATABASE_SERVICE,
            OracleTestDatabase.CDB_SERVICE,
            CoreConfig.DATABASE_USER,
            OracleTestDatabase.CAPTURE_USER,
            CoreConfig.DATABASE_PASSWORD,
            OracleTestDatabase.CAPTURE_PASSWORD,
            CoreConfig.DATABASE_PDBS,
            OracleTestDatabase.PDB1));
  }

  @Override
  public String describe() {
    return "container oracle-cdc-test-db from " + db.baseTag();
  }
}
