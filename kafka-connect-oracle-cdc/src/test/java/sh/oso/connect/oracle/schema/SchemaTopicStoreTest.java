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
package sh.oso.connect.oracle.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.schema.TableSchema;

class SchemaTopicStoreTest {

  static final TableId T = SchemaRecordsTest.T;

  final List<String> written = new ArrayList<>();
  long resume = 0;

  SchemaTopicStore store() {
    SchemaTopicStore s = new SchemaTopicStore(() -> resume);
    s.writeTo(
        new SchemaTopicStore.Writer() {
          public void versions(TableId t, List<TableSchema> v) {
            List<String> versions = new ArrayList<>();
            for (TableSchema x : v) {
              versions.add("v" + x.version());
            }
            written.add(t.table() + "=" + versions);
          }

          public void removed(TableId t) {
            written.add(t.table() + " removed");
          }
        });
    return s;
  }

  static TableSchema at(int version, long scn) {
    return SchemaRecordsTest.v1().withVersion(version, scn);
  }

  @Test
  void versionsBelowTheResumePointArePrunedExceptTheOneValidThere() {
    List<TableSchema> sorted = List.of(at(1, 0), at(2, 500), at(3, 900), at(4, 1500));
    assertThat(SchemaTopicStore.prune(sorted, 0)).containsExactlyElementsOf(sorted);
    assertThat(SchemaTopicStore.prune(sorted, 499)).containsExactlyElementsOf(sorted);
    assertThat(SchemaTopicStore.prune(sorted, 500))
        .containsExactly(sorted.get(1), sorted.get(2), sorted.get(3));
    assertThat(SchemaTopicStore.prune(sorted, 1000)).containsExactly(sorted.get(2), sorted.get(3));
    assertThat(SchemaTopicStore.prune(sorted, 9999)).containsExactly(sorted.get(3));
  }

  @Test
  void eachSaveWritesTheTablesWholeHistoryAndARemoveWritesATombstone() {
    SchemaTopicStore s = store();
    s.save(at(1, 0));
    resume = 600;
    s.save(at(2, 500));
    s.save(at(3, 700));
    assertThat(written).containsExactly("ORDERS=[v1]", "ORDERS=[v2]", "ORDERS=[v2, v3]");
    assertThat(s.load(T)).contains(at(3, 700));
    assertThat(s.versions(T)).containsExactly(at(2, 500), at(3, 700));

    s.remove(T);
    s.remove(T); // nothing left to remove: nothing written
    assertThat(written).endsWith("ORDERS removed");
    assertThat(written).hasSize(4);
    assertThat(s.load(T)).isEmpty();
  }

  @Test
  void heldChangesAreWrittenOnReleaseAsEachTableStandsThen() {
    // the task reads layouts in start(), before poll() can drain the record queue
    SchemaTopicStore s = store();
    s.hold();
    s.save(at(1, 0));
    s.save(at(2, 500));
    assertThat(s.load(T)).as("kept while held").contains(at(2, 500));
    assertThat(written).isEmpty();
    s.release();
    assertThat(written)
        .as("one record with the table's history")
        .containsExactly("ORDERS=[v1, v2]");
    s.save(at(3, 700));
    assertThat(written).as("written at once after the release").endsWith("ORDERS=[v1, v2, v3]");

    written.clear();
    s.hold();
    s.save(at(4, 800));
    s.remove(T);
    s.release();
    assertThat(written).as("removed while held: a tombstone").containsExactly("ORDERS removed");
  }

  @Test
  void seededVersionsAreNotWrittenBackAndATombstoneClearsTheTable() {
    SchemaTopicStore s = store();
    s.seed(T, List.of(at(3, 700), at(2, 500)));
    assertThat(written).isEmpty();
    assertThat(s.tables()).containsExactly(T);
    assertThat(s.versions(T)).containsExactly(at(2, 500), at(3, 700));
    assertThat(s.load(T)).contains(at(3, 700));
    s.seed(T, List.of());
    assertThat(s.tables()).isEmpty();
    assertThat(s.load(T)).isEmpty();
  }
}
