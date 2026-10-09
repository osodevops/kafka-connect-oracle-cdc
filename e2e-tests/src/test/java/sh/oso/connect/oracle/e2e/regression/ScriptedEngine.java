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
package sh.oso.connect.oracle.e2e.regression;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.buffer.HeapTransactionBuffer;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.engine.ChangeDecoder;
import sh.oso.connect.oracle.core.engine.EngineSettings;
import sh.oso.connect.oracle.core.engine.EventSink;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.DictionaryReader;
import sh.oso.connect.oracle.core.schema.KeySelector;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.SchemaRegistry;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.core.testkit.FakeCatalog;
import sh.oso.connect.oracle.core.testkit.FakeLogMiner;
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;

/**
 * The real engine and decoder over a scripted {@link FakeLogMiner} (ADR-0009), for regression cases
 * best proven without a database: {@code ORDERS(ID NUMBER key, NAME VARCHAR2)}, a sink that copies
 * each committed transaction and records the resume point of every step.
 */
final class ScriptedEngine {

  static final TableId ORDERS = FakeLogMiner.DEFAULT_TABLE;

  final FakeLogMiner fake = new FakeLogMiner().startAt(1000);
  final List<CommittedTransaction> committed = new ArrayList<>();
  long safeEnd;
  long minedTo;
  RedoRecordId resume;

  /** What a reconnect returns; by default a scenario has none. */
  CaptureEngine.Reconnector reconnector =
      cause -> {
        throw new AssertionError("this scenario has no reconnect", cause);
      };

  /** An INSERT of ORDERS as LogMiner writes it. */
  static String insert(int id, String name) {
    return "insert into \"APP\".\"ORDERS\"(\"ID\",\"NAME\") values ('" + id + "','" + name + "')";
  }

  CaptureEngine engine(int maxConsecutiveRetries) {
    return engine(maxConsecutiveRetries, Position.initial(1000, new DatabaseIdentity(1, 1)));
  }

  CaptureEngine engine(int maxConsecutiveRetries, Position start) {
    InMemorySchemaStore store = new InMemorySchemaStore();
    store.save(
        new TableSchema(
            ORDERS,
            List.of(
                ColumnSpec.of("ID", 1, OracleType.NUMBER),
                ColumnSpec.of("NAME", 2, OracleType.VARCHAR2)),
            List.of("ID"),
            KeySource.PRIMARY_KEY,
            true,
            false));
    DictionaryReader none =
        new DictionaryReader() {
          public Optional<TableSchema> read(TableId t) {
            throw new IllegalStateException("the dictionary is not read: " + t);
          }

          public KeySelector.Candidates keyCandidates(TableId t) {
            throw new IllegalStateException("the dictionary is not read: " + t);
          }
        };
    EventSink sink =
        new EventSink() {
          public void committed(CommittedTransaction tx, int skip, RedoRecordId at) {
            committed.add(
                new CommittedTransaction(
                    tx.key(),
                    tx.firstCaptured(),
                    tx.startId(),
                    tx.commitId(),
                    tx.commitTimestamp(),
                    tx.thread(),
                    tx.username(),
                    tx.clientId(),
                    new ArrayList<>(tx.events().subList(skip, tx.size()))));
          }

          public void stepApplied(long scn, RedoRecordId at) {
            minedTo = scn;
            resume = at;
          }

          @Override
          public void idle(long scn, RedoRecordId at) {
            stepApplied(scn, at);
          }
        };
    return new CaptureEngine(
        start,
        fake,
        new LogInventory(
            new FakeCatalog().archivedRun(1, 1, 40, 1000, 100), CoreConfig.CaptureMode.ONLINE, 1),
        () -> safeEnd,
        new HeapTransactionBuffer(),
        new SchemaRegistry(
            store, none, new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.FAIL)),
        ChangeDecoder.rowDecoder(),
        sink,
        new EngineSettings(
            Duration.ofSeconds(2),
            8,
            Duration.ofHours(1),
            Duration.ofMillis(10),
            maxConsecutiveRetries,
            CoreConfig.DecodeErrorAction.FAIL),
        new OraErrorClassifier(),
        Set.of("APP"),
        () -> Set.of("APP"),
        cause -> reconnector.reconnect(cause),
        () -> Instant.EPOCH);
  }

  /** Steps until the engine is idle at the safe end. */
  void runUntilIdle(CaptureEngine e) throws Exception {
    for (int i = 0; i < 1000; i++) {
      if (e.runOnce() == CaptureEngine.Progress.IDLE) {
        return;
      }
    }
    throw new AssertionError("the engine did not go idle");
  }
}
