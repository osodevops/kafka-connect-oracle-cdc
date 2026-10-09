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
package sh.oso.connect.oracle.e2e.qual;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Optional;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.logs.DictionaryBuild;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.topology.JdbcCatalogSource;
import sh.oso.connect.oracle.core.topology.TopologyProbe;
import sh.oso.connect.oracle.e2e.support.Evidence;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.TestDatabase;

/**
 * Qualification (T4): the capture user can write a dictionary build into the redo (the lag case
 * needs one, PRD-00 CORE-DICT), and the log inventory finds it once its logs are archived.
 */
@Tag("qual")
class DictionaryBuildQualIT {

  private final TestDatabase db = TestDatabase.get();

  @Test
  void theCaptureUserWritesADictionaryBuildTheInventoryFinds() throws Exception {
    Evidence ev = Evidence.of(getClass(), "dictionary build").param("target", db.describe());
    try (Connection c = db.capture()) {
      long before = LogMinerHelper.currentScn(c);
      try (Statement s = c.createStatement()) {
        s.execute("BEGIN DBMS_LOGMNR_D.BUILD(OPTIONS => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;");
      }
      db.archiveLogCurrent();
      JdbcCatalogSource catalog = new JdbcCatalogSource(c);
      int dest = new TopologyProbe(catalog, null).probe().archiveDestId();
      Optional<DictionaryBuild> build =
          new LogInventory(catalog, CaptureMode.ONLINE, dest)
              .dictionaryBuildBefore(LogMinerHelper.currentScn(c));
      assertThat(build).isPresent();
      assertThat(build.get().firstScn()).isGreaterThanOrEqualTo(before);
      ev.param("buildFirstScn", build.get().firstScn());
    } catch (Throwable t) {
      ev.failed(t);
      throw t;
    } finally {
      ev.write();
    }
  }
}
