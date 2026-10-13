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
package sh.oso.connect.oracle.envelope.confluent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.envelope.confluent.FieldLayout.Meta;

class FieldLayoutTest {
  @Test
  void nestsInFirstMemberOrderAndOmitsNullHeaders() {
    FieldLayout layout =
        new FieldLayout(
            Map.of(
                Meta.TABLE,
                "table",
                Meta.SCN,
                "header:scn",
                Meta.OP_TS,
                "meta.time.op",
                Meta.CURRENT_TS,
                "meta.time.current",
                Meta.ROW_ID,
                "header:row"));
    SchemaBuilder builder = SchemaBuilder.struct();
    layout.addTo(builder);
    Struct value = new Struct(builder.build());
    Map<Meta, String> metadata =
        Map.of(
            Meta.TABLE, "PDB.APP.T", Meta.SCN, "100", Meta.OP_TS, "2000", Meta.CURRENT_TS, "3000");
    layout.fillBody(value, metadata);
    ConnectHeaders headers = new ConnectHeaders();
    layout.fillHeaders(headers, metadata);
    assertThat(layout.topLevelNames()).containsExactly("table", "meta");
    assertThat(value.getStruct("meta").getStruct("time").getString("op")).isEqualTo("2000");
    assertThat(value.getStruct("meta").getStruct("time").getString("current")).isEqualTo("3000");
    assertThat(headers).hasSize(1);
    assertThat(headers.lastWithName("scn").value()).isEqualTo("100");
    assertThat(layout.wants(Meta.SCN)).isTrue();
    assertThat(layout.wants(Meta.REDO)).isFalse();
  }

  @Test
  void refusesDuplicateBodyAndHeaderNamesAndBothDirectionsOfPathCollision() {
    for (Map<Meta, String> names :
        java.util.List.of(
            Map.of(Meta.TABLE, "x", Meta.SCN, "x"),
            Map.of(Meta.TABLE, "header:x", Meta.SCN, "header:x"),
            Map.of(Meta.TABLE, "x", Meta.SCN, "x.y"),
            Map.of(Meta.TABLE, "x.y", Meta.SCN, "x"))) {
      assertThatThrownBy(() -> new FieldLayout(names)).isInstanceOf(IllegalArgumentException.class);
    }
  }
}
