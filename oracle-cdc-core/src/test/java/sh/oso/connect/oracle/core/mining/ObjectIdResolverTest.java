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
package sh.oso.connect.oracle.core.mining;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.testkit.FakeObjectCatalog;

class ObjectIdResolverTest {

  private final FakeObjectCatalog cat =
      new FakeObjectCatalog()
          .table(3, "FREEPDB1", "APP", "ORDERS", 1001)
          .partition(3, "FREEPDB1", "APP", "EVENTS", "P1", 1101)
          .partition(3, "FREEPDB1", "APP", "EVENTS", "P2", 1102)
          .table(3, "FREEPDB1", "APP", "EVENTS", 1100)
          .iotTop(3, "FREEPDB1", "APP", "LOOKUP", 1201)
          .table(3, "FREEPDB1", "APP", "LOOKUP", 1200)
          .table(3, "FREEPDB1", "APP", "SCRATCH", 1300)
          .table(3, "FREEPDB1", "HR", "EMP", 2001)
          .table(4, "FREEPDB2", "APP", "ORDERS", 3001);

  @Test
  void includesEveryObjectIdOfAMatchedTableAndCollectsOwners() throws Exception {
    ObjectIdResolver r =
        new ObjectIdResolver(
            cat,
            List.of("freepdb1\\.app\\..*"),
            List.of("FREEPDB1\\.APP\\.SCRATCH"),
            List.of("FREEPDB1"),
            false);
    ResolvedObjects o = r.resolve();
    assertThat(o.idsByContainer()).containsOnlyKeys(3);
    assertThat(o.idsByContainer().get(3))
        .containsExactlyInAnyOrder(1001L, 1100L, 1101L, 1102L, 1200L, 1201L);
    assertThat(o.byObject().get(new ObjectKey(3, 1102L)))
        .isEqualTo(new TableId("FREEPDB1", "APP", "EVENTS"));
    assertThat(o.byObject().get(new ObjectKey(3, 1201L)))
        .isEqualTo(new TableId("FREEPDB1", "APP", "LOOKUP"));
    assertThat(o.tables()).hasSize(3);
    assertThat(o.owners()).containsExactly("APP");
    assertThat(o.filter(Set.of("X"), 1000).objectIdsByContainer().get(3)).hasSize(6);
    assertThat(o.objectCount()).isEqualTo(6);
    assertThat(o.isEmpty()).isFalse();
  }

  @Test
  void pdbListRestrictsContainersAndCaseSensitivityIsOptional() throws Exception {
    ResolvedObjects both =
        new ObjectIdResolver(cat, List.of(".*\\.APP\\.ORDERS"), List.of(), List.of(), false)
            .resolve();
    assertThat(both.byObject().keySet())
        .containsExactlyInAnyOrder(new ObjectKey(3, 1001L), new ObjectKey(4, 3001L));
    assertThat(both.idsByContainer()).containsOnlyKeys(3, 4);
    ResolvedObjects one =
        new ObjectIdResolver(
                cat, List.of(".*\\.APP\\.ORDERS"), List.of(), List.of("freepdb2"), false)
            .resolve();
    assertThat(one.byObject().keySet()).containsExactly(new ObjectKey(4, 3001L));
    ResolvedObjects cs =
        new ObjectIdResolver(cat, List.of("freepdb1\\.app\\.orders"), List.of(), List.of(), true)
            .resolve();
    assertThat(cs.isEmpty()).isTrue();
  }

  @Test
  void noIncludePatternMeansEveryUserTable() throws Exception {
    ResolvedObjects all =
        new ObjectIdResolver(cat, List.of(), List.of(), List.of(), false).resolve();
    assertThat(all.tables()).hasSize(6);
    assertThat(all.owners()).containsExactly("APP", "HR");
  }
}
