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

import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.errors.TopologyException;
import sh.oso.connect.oracle.core.testkit.FakeCatalog;

class TopologyProbeTest {

  @Test
  void choosesTheLowestValidLocalDestinationByDefault() throws Exception {
    FakeCatalog cat = new FakeCatalog();
    cat.destinations.clear();
    cat.destinations.add(
        new ArchiveDestination(2, "LOG_ARCHIVE_DEST_2", "/b", "VALID", "LOCAL", "PRIMARY"));
    cat.destinations.add(
        new ArchiveDestination(3, "LOG_ARCHIVE_DEST_3", "standby", "VALID", "PHYSICAL", "STANDBY"));
    cat.destinations.add(
        new ArchiveDestination(1, "LOG_ARCHIVE_DEST_1", "/a", "ERROR", "LOCAL", "PRIMARY"));
    Topology t = new TopologyProbe(cat, null).probe();
    assertThat(t.archiveDestId()).isEqualTo(2);
    assertThat(t.rac()).isFalse();
    assertThat(t.fingerprint()).contains("threads=1").contains("pdbs=FREEPDB1").contains("dest=2");
  }

  @Test
  void configuredDestinationByNameOrError() throws Exception {
    FakeCatalog cat = new FakeCatalog();
    assertThat(new TopologyProbe(cat, "log_archive_dest_1").probe().archiveDestId()).isEqualTo(1);
    assertThatThrownBy(() -> new TopologyProbe(cat, "LOG_ARCHIVE_DEST_9").probe())
        .isInstanceOf(TopologyException.class)
        .hasMessageContaining("LOG_ARCHIVE_DEST_9");
    cat.destinations.clear();
    assertThatThrownBy(() -> new TopologyProbe(cat, null).probe())
        .isInstanceOf(TopologyException.class)
        .hasMessageContaining("No valid local");
  }

  @Test
  void fingerprintChangesWithThreadsAndPdbs() throws Exception {
    FakeCatalog cat = new FakeCatalog();
    String before = new TopologyProbe(cat, null).probe().fingerprint();
    cat.threads.add(new ThreadInfo(2, true, "OPEN", 1));
    String withRac = new TopologyProbe(cat, null).probe().fingerprint();
    assertThat(withRac).isNotEqualTo(before);
    assertThat(new TopologyProbe(cat, null).probe().rac()).isTrue();
    cat.pdbs.add(new PdbInfo(4, "FREEPDB2", "MOUNTED", 11L));
    assertThat(new TopologyProbe(cat, null).probe().fingerprint()).isEqualTo(withRac);
    assertThat(cat.database().identity()).isEqualTo("1234@1");
  }
}
