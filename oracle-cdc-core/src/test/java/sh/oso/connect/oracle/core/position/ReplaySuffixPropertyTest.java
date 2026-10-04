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
package sh.oso.connect.oracle.core.position;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tuple;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.model.RowChange;

/**
 * ADR-0010 property: for any commit sequence and any acknowledgement point, replaying the whole
 * sequence under the position recorded at that point yields exactly the unacknowledged suffix.
 */
class ReplaySuffixPropertyTest {

  record Scenario(List<CommittedTransaction> commits, int ackedTx, int ackedEvents) {}

  @Property(tries = 300)
  void replayYieldsExactlyTheUnacknowledgedSuffix(@ForAll("scenarios") Scenario s) {
    List<RowChange> all = new ArrayList<>();
    for (CommittedTransaction t : s.commits()) {
      all.addAll(t.events());
    }
    int ackedTotal = 0;
    for (int i = 0; i < s.ackedTx(); i++) {
      ackedTotal += s.commits().get(i).size();
    }
    ackedTotal += s.ackedEvents();
    CommittedTransaction acked = s.commits().get(s.ackedTx());
    Position p =
        Position.initial(1, new DatabaseIdentity(1, 1))
            .withCommit(acked.commitScn(), acked.thread(), acked.key(), s.ackedEvents());

    List<RowChange> replayed = new ArrayList<>();
    for (CommittedTransaction t : s.commits()) {
      int skip = SkipRule.eventsToSkip(p, t);
      replayed.addAll(t.events().subList(skip, t.size()));
    }
    assertThat(replayed).isEqualTo(all.subList(ackedTotal, all.size()));
  }

  @Provide
  Arbitrary<Scenario> scenarios() {
    // commit SCNs may tie across threads and keys; keys are unique per scenario
    Arbitrary<List<Tuple.Tuple3<Integer, Integer, Integer>>> shape =
        Combinators.combine(
                Arbitraries.integers().between(0, 3), // scn increment (0 = tie)
                Arbitraries.integers().between(1, 2), // thread
                Arbitraries.integers().between(1, 5)) // events
            .as(Tuple::of)
            .list()
            .ofMinSize(1)
            .ofMaxSize(12);
    return shape.flatMap(
        list -> {
          List<CommittedTransaction> commits = new ArrayList<>();
          long scn = 100;
          long sqn = 1;
          for (Tuple.Tuple3<Integer, Integer, Integer> t : list) {
            scn += t.get1();
            commits.add(ResumeAndSkipTest.tx(scn, t.get2(), sqn++, t.get3()));
          }
          commits.sort(CommitOrder.COMPARATOR);
          return Arbitraries.integers()
              .between(0, commits.size() - 1)
              .flatMap(
                  ackedTx ->
                      Arbitraries.integers()
                          .between(0, commits.get(ackedTx).size())
                          .map(ackedEvents -> new Scenario(commits, ackedTx, ackedEvents)));
        });
  }
}
