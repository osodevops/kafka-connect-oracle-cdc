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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.errors.OracleCdcCorruptionException;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

class PositionCodecTest {

  @SuppressWarnings("unchecked")
  private static Map<String, Object> fixture(String name) throws Exception {
    return new ObjectMapper()
        .readValue(Files.readString(Path.of("src/test/resources/position/" + name)), Map.class);
  }

  @Test
  void readsTheV1FixtureAndRoundTrips() throws Exception {
    Map<String, Object> m = fixture("position-v1.json");
    Position p = PositionCodec.read(m);
    assertThat(p.version()).isEqualTo(1);
    assertThat(p.resumeScn()).isEqualTo(123456789L);
    assertThat(p.lastCommitScn()).isEqualTo(123456800L);
    assertThat(p.lastCommitKey()).isEqualTo(new TxKey(3, new Xid(7, 21, 1042)));
    assertThat(p.lastCommitThread()).isEqualTo(1);
    assertThat(p.eventIndex()).isEqualTo(2);
    assertThat(p.journalGeneration()).isEqualTo(4);
    assertThat(p.identity()).isEqualTo(new DatabaseIdentity(1234567890L, 1));
    assertThat(p.released()).containsExactly("3:9.1.77");
    assertThat(p.snapshot()).isNull();
    assertThat(p.extras()).isEmpty();
    Map<String, Object> written = PositionCodec.write(p);
    assertThat(written)
        .containsAllEntriesOf(
            Map.of(
                "v",
                1,
                "resume_scn",
                123456789L,
                "last_commit_xid",
                "3:7.21.1042",
                "event_index",
                2));
    assertThat(PositionCodec.read(written)).isEqualTo(p);
    // Connect accepts only primitive offset values
    for (Object v : written.values()) {
      assertThat(v == null || v instanceof String || v instanceof Number || v instanceof Boolean)
          .as("offset value %s", v)
          .isTrue();
    }
    assertThat(written.get("released_xids")).isEqualTo("3:9.1.77");
    assertThat(
            PositionCodec.read(Map.of("v", 1, "released_xids", List.of("a:1.2.3", "b:4.5.6")))
                .released())
        .as("the pre-release list form still reads")
        .containsExactly("a:1.2.3", "b:4.5.6");
  }

  @Test
  void unknownFieldsSurviveARoundTripAndSnapshotIsOpaque() throws Exception {
    Position p = PositionCodec.read(fixture("position-v1-with-unknown-field.json"));
    assertThat(p.extras()).containsEntry("future_field", "kept by a one-version-older reader");
    assertThat(p.snapshot()).containsEntry("mode", "chunk");
    Map<String, Object> written = PositionCodec.write(p.withResumeScn(501));
    assertThat(written).containsEntry("future_field", "kept by a one-version-older reader");
    assertThat(written).containsEntry("resume_scn", 501L);
    assertThat(written.get("snapshot")).isInstanceOf(String.class).asString().contains("chunk");
    assertThat(PositionCodec.read(written).snapshot()).containsEntry("frontier", "9");
    assertThatThrownBy(() -> PositionCodec.read(Map.of("v", 1, "snapshot", "{not json")))
        .isInstanceOf(OracleCdcCorruptionException.class);
  }

  @Test
  void refusesEmptyUnversionedAndNewerOffsets() {
    assertThatThrownBy(() -> PositionCodec.read(Map.of()))
        .isInstanceOf(OracleCdcCorruptionException.class);
    assertThatThrownBy(() -> PositionCodec.read(null))
        .isInstanceOf(OracleCdcCorruptionException.class);
    assertThatThrownBy(() -> PositionCodec.read(Map.of("scn", 5)))
        .isInstanceOf(OracleCdcCorruptionException.class)
        .hasMessageContaining("no format version");
    assertThatThrownBy(() -> PositionCodec.read(Map.of("v", 2, "resume_scn", 5)))
        .isInstanceOf(OracleCdcCorruptionException.class)
        .hasMessageContaining("version 2");
  }

  @Test
  void toleratesStringNumbersAndLegacyKeysWithoutContainer() {
    Map<String, Object> m = new HashMap<>();
    m.put("v", "1");
    m.put("resume_scn", "77");
    m.put("last_commit_scn", 80);
    m.put("last_commit_xid", "7.1.1");
    m.put("dbid", 1L);
    Position p = PositionCodec.read(m);
    assertThat(p.resumeScn()).isEqualTo(77);
    assertThat(p.lastCommitKey()).isEqualTo(new TxKey(0, new Xid(7, 1, 1)));
    assertThat(p.identity().resetlogsScn()).isZero();
  }

  @Test
  void initialPositionAndMutators() {
    Position p = Position.initial(1000, new DatabaseIdentity(1, 2));
    assertThat(p.hasCommit()).isFalse();
    assertThat(p.version()).isEqualTo(Position.CURRENT_VERSION);
    TxKey k = new TxKey(3, new Xid(1, 1, 1));
    Position c =
        p.withCommit(1500, 1, k, 3).withJournalGeneration(2).withReleased(List.of("3:1.1.9"));
    assertThat(c.hasCommit()).isTrue();
    assertThat(c.lastCommitScn()).isEqualTo(1500);
    assertThat(c.eventIndex()).isEqualTo(3);
    assertThat(c.journalGeneration()).isEqualTo(2);
    assertThat(c.released()).containsExactly("3:1.1.9");
    assertThat(c.resumeScn()).isEqualTo(1000);
    assertThatThrownBy(
            () ->
                new Position(
                    0,
                    1,
                    1,
                    null,
                    0,
                    0,
                    0,
                    0,
                    new DatabaseIdentity(1, 1),
                    List.of(),
                    null,
                    Map.of()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> p.withResumeScn(-1)).isInstanceOf(IllegalArgumentException.class);
  }

  // ADR-0026: the per-thread block

  static final DatabaseIdentity IDENT = new DatabaseIdentity(42, 7);

  static sh.oso.connect.oracle.core.model.RedoRecordId rba(long scn, long block) {
    return new sh.oso.connect.oracle.core.model.RedoRecordId(
        scn, String.format(" 0x%06x.%08x.%04x ", 1, block, 0), 0);
  }

  static Position twoThreads() {
    TxKey k1 = new TxKey(0, Xid.parse("0001.002.00000003"));
    TxKey k2 = new TxKey(0, Xid.parse("0004.005.00000006"));
    java.util.SortedMap<Integer, ThreadMark> marks = new java.util.TreeMap<>();
    marks.put(1, new ThreadMark(rba(100, 9), rba(120, 12), k1));
    marks.put(2, new ThreadMark(rba(90, 3), rba(130, 5), k2));
    return Position.initial(90, IDENT).withCommit(rba(130, 5), 2, k2, 4).withThreads(marks);
  }

  @Test
  void aSingleThreadPositionWritesNoThreadsKey() {
    Position p =
        Position.initial(100, IDENT)
            .withCommit(rba(120, 12), 1, new TxKey(0, Xid.parse("0001.002.00000003")), 2)
            .withResume(rba(110, 10));
    Map<String, Object> written = PositionCodec.write(p);
    assertThat(written).doesNotContainKey("threads");
    assertThat(PositionCodec.write(PositionCodec.read(written))).isEqualTo(written);
  }

  @Test
  void thePerThreadBlockRoundTrips() {
    Position p = twoThreads();
    Map<String, Object> written = PositionCodec.write(p);
    assertThat(written).containsKey("threads");
    Position back = PositionCodec.read(written);
    assertThat(back.threads()).isEqualTo(p.threads());
    assertThat(back.extras()).doesNotContainKey("threads");
    assertThat(PositionCodec.write(back)).isEqualTo(written);
  }

  @Test
  void aBlockThatDisagreesWithTheLegacyKeysIsIgnored() {
    Map<String, Object> written = new HashMap<>(PositionCodec.write(twoThreads()));
    // an older version advanced the legacy commit and carried the block along unchanged
    written.put("last_commit_xid", "0:0009.009.00000009");
    assertThat(PositionCodec.read(written).perThread()).isFalse();
    Map<String, Object> moved = new HashMap<>(PositionCodec.write(twoThreads()));
    moved.put("resume_scn", 95L);
    assertThat(PositionCodec.read(moved).perThread()).isFalse();
  }

  @Test
  void anUnreadableBlockStopsTheTask() {
    Map<String, Object> written = new HashMap<>(PositionCodec.write(twoThreads()));
    written.put("threads", "{not json");
    assertThatThrownBy(() -> PositionCodec.read(written))
        .isInstanceOf(OracleCdcCorruptionException.class);
  }
}
