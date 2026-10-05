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
package sh.oso.connect.oracle.ops;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;

class OpsEventWriterTest {

  @Test
  void writesAVersionedStructKeyedByServerWithThePositionAsOffset() {
    OpsEventWriter w = new OpsEventWriter("cdc.cdc.ops", "cdc", Map.of("server", "cdc"));
    Position p = Position.initial(5000, new DatabaseIdentity(77, 1));
    SourceRecord r =
        w.record(OpsEvent.of(OpsEvent.Type.DDL_SEEN, 1234L, 5000L, "owner", "APP", "sql", null), p);
    assertThat(r.topic()).isEqualTo("cdc.cdc.ops");
    assertThat(r.sourcePartition()).isEqualTo(Map.of("server", "cdc"));
    assertThat(PositionCodec.read(r.sourceOffset())).isEqualTo(p);
    assertThat(r.timestamp()).isEqualTo(1234L);
    assertThat(((Struct) r.key()).getString("server")).isEqualTo("cdc");
    Struct v = (Struct) r.value();
    assertThat(v.schema().version()).isEqualTo(OpsEvent.SCHEMA_VERSION);
    assertThat(v.getInt32("v")).isEqualTo(OpsEvent.SCHEMA_VERSION);
    assertThat(v.getString("type")).isEqualTo("ddl-seen");
    assertThat(v.getInt64("ts_ms")).isEqualTo(1234L);
    assertThat(v.getInt64("resume_scn")).isEqualTo(5000L);
    assertThat(v.getMap("details")).containsExactly(Map.entry("owner", "APP"));
  }

  @Test
  void everyTypeHasAStableLowerCaseWireName() {
    for (OpsEvent.Type t : OpsEvent.Type.values()) {
      assertThat(t.wire()).matches("[a-z]+(-[a-z]+)*");
    }
    assertThat(OpsEvent.Type.STOP.wire()).isEqualTo("stop");
    assertThat(OpsEvent.Type.TRANSACTION_ORPHAN_RELEASED.wire())
        .isEqualTo("transaction-orphan-released");
  }

  @Test
  void aNullOffsetIsAllowedForEventsOutsideThePositionStream() {
    OpsEventWriter w = new OpsEventWriter("t", "cdc", Map.of("server", "cdc"));
    SourceRecord r = w.record(OpsEvent.of(OpsEvent.Type.STARTUP, 1L, null), null);
    assertThat(r.sourceOffset()).isNull();
    assertThat(((Struct) r.value()).get("resume_scn")).isNull();
  }
}
