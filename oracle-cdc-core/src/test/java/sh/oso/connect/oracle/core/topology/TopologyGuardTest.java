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
package sh.oso.connect.oracle.core.topology;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.errors.TopologyException;

class TopologyGuardTest {

  private static DatabaseInfo db(String role, String open) {
    return new DatabaseInfo(
        1, "CDB", true, "ARCHIVELOG", open, role, "19.0.0.0.0", 1, true, "Linux");
  }

  private static Topology topology(DatabaseInfo db, ThreadInfo... threads) {
    return new Topology(db, List.of(threads), List.of(), 1);
  }

  private static final ThreadInfo T1 = new ThreadInfo(1, true, "OPEN", 10);

  @Test
  void onlineModeNeedsThePrimaryOpenReadWrite() {
    assertThat(TopologyGuard.roleRefusal(db("PRIMARY", "READ WRITE"), CaptureMode.ONLINE)).isNull();
    for (DatabaseInfo d :
        List.of(
            db("PHYSICAL STANDBY", "READ ONLY WITH APPLY"),
            db("PHYSICAL STANDBY", "MOUNTED"),
            db("PRIMARY", "READ ONLY"),
            db("LOGICAL STANDBY", "READ WRITE"))) {
      assertThat(TopologyGuard.roleRefusal(d, CaptureMode.ONLINE))
          .as("%s %s", d.databaseRole(), d.openMode())
          .contains("cdc.capture.mode=online needs a PRIMARY database open READ WRITE");
    }
  }

  @Test
  void archiveOnlyModeAlsoTakesAPhysicalStandbyOpenReadOnly() {
    for (DatabaseInfo d :
        List.of(
            db("PRIMARY", "READ WRITE"),
            db("PHYSICAL STANDBY", "READ ONLY WITH APPLY"),
            db("PHYSICAL STANDBY", "READ ONLY"))) {
      assertThat(TopologyGuard.roleRefusal(d, CaptureMode.ARCHIVE_ONLY))
          .as("%s %s", d.databaseRole(), d.openMode())
          .isNull();
    }
    assertThat(
            TopologyGuard.roleRefusal(db("PHYSICAL STANDBY", "MOUNTED"), CaptureMode.ARCHIVE_ONLY))
        .contains("no dictionary to read");
    assertThat(
            TopologyGuard.roleRefusal(
                db("LOGICAL STANDBY", "READ WRITE"), CaptureMode.ARCHIVE_ONLY))
        .contains("redo and SCNs of its own");
    assertThat(
            TopologyGuard.roleRefusal(
                db("SNAPSHOT STANDBY", "READ WRITE"), CaptureMode.ARCHIVE_ONLY))
        .contains("redo and SCNs of its own");
    assertThat(TopologyGuard.applyStopped(db("PHYSICAL STANDBY", "READ ONLY"))).isTrue();
    assertThat(TopologyGuard.applyStopped(db("PHYSICAL STANDBY", "READ ONLY WITH APPLY")))
        .isFalse();
  }

  @Test
  void theStartIsRefusedWithCdc5001ForEitherReason() {
    assertThatThrownBy(
            () ->
                TopologyGuard.requireQualified(
                    topology(db("PHYSICAL STANDBY", "READ ONLY WITH APPLY"), T1),
                    CaptureMode.ONLINE))
        .isInstanceOf(TopologyException.class)
        .hasMessageContaining("CDC-5001")
        .hasMessageContaining("archive_only");
    assertThatThrownBy(
            () ->
                TopologyGuard.requireQualified(
                    topology(db("PRIMARY", "READ WRITE"), T1, new ThreadInfo(2, true, "OPEN", 4)),
                    CaptureMode.ONLINE))
        .isInstanceOf(TopologyException.class)
        .hasMessageContaining("2 enabled redo threads");
    // a qualified standby with one thread passes
    TopologyGuard.requireQualified(
        topology(db("PHYSICAL STANDBY", "READ ONLY WITH APPLY"), T1), CaptureMode.ARCHIVE_ONLY);
  }
}
