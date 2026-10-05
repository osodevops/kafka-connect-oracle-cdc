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
package sh.oso.connect.oracle.core.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.TransactionBuffer;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.config.CoreConfig.DecodeErrorAction;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.errors.OrphanReleaseViolationException;
import sh.oso.connect.oracle.core.errors.TransactionTooOldException;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.Position;

/** CORE-TX-6: the long transaction policy is never silent. */
class TransactionAgeEngineTest {

  static final class DiscardSink extends CaptureEngineTest.Sink {
    final List<TransactionBuffer.OpenTransaction> discarded = new ArrayList<>();
    List<String> released = List.of();

    @Override
    public void transactionDiscarded(
        TransactionBuffer.OpenTransaction tx, Duration age, List<String> ledger) {
      discarded.add(tx);
      released = ledger;
    }
  }

  private static CaptureEngine engine(
      CaptureEngineTest.Harness h,
      DiscardSink sink,
      Position start,
      EngineSettings.MaxAgeAction action) {
    EngineSettings s =
        new EngineSettings(
            Duration.ofSeconds(2),
            8,
            Duration.ofHours(1),
            Duration.ofMillis(10),
            3,
            DecodeErrorAction.FAIL,
            Duration.ofMinutes(5),
            action);
    return new CaptureEngine(
        start,
        h.fake,
        new LogInventory(h.catalog, CaptureMode.ONLINE, 1),
        () -> h.safeEnd,
        h.buffer,
        CaptureEngineTest.registry(),
        CaptureEngineTest.DECODER,
        sink,
        s,
        new OraErrorClassifier(),
        Set.of("APP"),
        () -> Set.of("APP"),
        cause ->
            new CaptureEngine.Sources(
                h.fake, new LogInventory(h.catalog, CaptureMode.ONLINE, 1), () -> h.safeEnd),
        () -> Instant.ofEpochMilli(h.clock.getAsLong()));
  }

  @Test
  void discardDropsTheTransactionWithAnEventAndItsLateCommitIsAStop() throws Exception {
    CaptureEngineTest.Harness h = new CaptureEngineTest.Harness();
    DiscardSink sink = new DiscardSink();
    TxKey a = h.fake.tx(1, 1, 1);
    TxKey b = h.fake.tx(1, 1, 2);
    h.fake
        .start(a, "APP")
        .insert(a, CaptureEngineTest.T, "a1")
        .start(b, "APP")
        .insert(b, CaptureEngineTest.T, "b1")
        .commit(b);
    h.safeEnd = h.fake.nextScn();
    CaptureEngine e = engine(h, sink, h.initial(), EngineSettings.MaxAgeAction.DISCARD);
    h.runUntilIdle(e);
    assertThat(sink.sqls()).containsExactly("b1");
    assertThat(h.buffer.openTransactions()).isEqualTo(1);
    h.clock.advance(Duration.ofMinutes(4));
    h.runUntilIdle(e);
    assertThat(sink.discarded).as("younger than the limit").isEmpty();
    h.clock.advance(Duration.ofMinutes(2));
    h.runUntilIdle(e);
    assertThat(sink.discarded).hasSize(1);
    assertThat(sink.discarded.get(0).key()).isEqualTo(a);
    assertThat(sink.discarded.get(0).events()).isEqualTo(1);
    assertThat(sink.released).containsExactly(a.toString());
    assertThat(h.buffer.openTransactions()).isZero();
    assertThat(e.metrics().transactionsDiscarded.get()).isEqualTo(1);
    assertThat(h.buffer.oldestFirstCaptured()).as("no longer pins the position").isEmpty();
    // rows of the discarded transaction that arrive later never form a partial emission
    h.fake.insert(a, CaptureEngineTest.T, "a2").commit(a);
    h.safeEnd = h.fake.nextScn();
    assertThatThrownBy(() -> h.runUntilIdle(e))
        .isInstanceOf(OrphanReleaseViolationException.class)
        .hasMessageContaining("CDC-7001");
    assertThat(sink.sqls()).containsExactly("b1");
  }

  @Test
  void failStopsTheTaskNamingTheTransaction() throws Exception {
    CaptureEngineTest.Harness h = new CaptureEngineTest.Harness();
    DiscardSink sink = new DiscardSink();
    TxKey a = h.fake.tx(1, 1, 1);
    h.fake.start(a, "APP").insert(a, CaptureEngineTest.T, "a1");
    h.safeEnd = h.fake.nextScn();
    CaptureEngine e = engine(h, sink, h.initial(), EngineSettings.MaxAgeAction.FAIL);
    h.runUntilIdle(e);
    h.clock.advance(Duration.ofMinutes(6));
    assertThatThrownBy(() -> h.runUntilIdle(e))
        .isInstanceOf(TransactionTooOldException.class)
        .hasMessageContaining("CDC-4004")
        .hasMessageContaining(a.toString())
        .hasMessageContaining("cdc.transaction.max.age.ms");
    assertThat(sink.discarded).isEmpty();
    assertThat(h.buffer.openTransactions()).as("nothing was dropped").isEqualTo(1);
  }

  @Test
  void noLimitMeansNoCheck() throws Exception {
    CaptureEngineTest.Harness h = new CaptureEngineTest.Harness();
    TxKey a = h.fake.tx(1, 1, 1);
    h.fake.start(a, "APP").insert(a, CaptureEngineTest.T, "a1");
    h.safeEnd = h.fake.nextScn();
    CaptureEngine e = h.engine(h.initial(), DecodeErrorAction.FAIL);
    h.runUntilIdle(e);
    h.clock.advance(Duration.ofDays(3));
    h.runUntilIdle(e);
    assertThat(h.buffer.openTransactions()).isEqualTo(1);
    assertThat(e.metrics().transactionsDiscarded.get()).isZero();
  }
}
