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
package sh.oso.connect.oracle.bench.check;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

class MaterialiserTest {

  static final ObjectMapper M = new ObjectMapper();

  static ConsumerRecord<String, String> record(
      long offset,
      String key,
      String op,
      String before,
      String after,
      String xid,
      long commitScn,
      int idx,
      int count) {
    String value =
        op == null
            ? null
            : "{\"op\":\""
                + op
                + "\",\"before\":"
                + (before == null ? "null" : before)
                + ",\"after\":"
                + (after == null ? "null" : after)
                + ",\"source\":{\"txId\":\""
                + xid
                + "\",\"commit_scn\":\""
                + commitScn
                + "\",\"scn\":\""
                + (commitScn - 1)
                + "\",\"db\":\"FREEPDB1\",\"schema\":\"APP\",\"table\":\"T\"}}";
    RecordHeaders h = new RecordHeaders();
    h.add("cdc.xid", xid.getBytes(StandardCharsets.UTF_8));
    h.add("cdc.event_index", Integer.toString(idx).getBytes(StandardCharsets.UTF_8));
    h.add("cdc.event_count", Integer.toString(count).getBytes(StandardCharsets.UTF_8));
    return new ConsumerRecord<>(
        "cdc.FREEPDB1.APP.T",
        0,
        offset,
        0L,
        TimestampType.CREATE_TIME,
        0,
        0,
        key,
        value,
        h,
        java.util.Optional.empty());
  }

  @Test
  void anUnavailablePlaceholderKeepsThePreviousValue() throws Exception {
    Materialiser m = new Materialiser();
    m.apply(
        new RecordJson(
            record(
                0,
                "{\"ID\":1}",
                "c",
                null,
                "{\"ID\":1,\"NOTE\":\"text\",\"B\":\"AQ==\"}",
                "1.1.1",
                100,
                0,
                1)));
    String blobPlaceholder =
        java.util.Base64.getEncoder()
            .encodeToString(
                Materialiser.DEFAULT_PLACEHOLDER.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    m.apply(
        new RecordJson(
            record(
                1,
                "{\"ID\":1}",
                "u",
                null,
                "{\"ID\":1,\"NOTE\":\"__cdc_unavailable_value\",\"B\":\""
                    + blobPlaceholder
                    + "\",\"N\":2}",
                "1.1.2",
                110,
                0,
                1)));
    assertThat(m.tables().values().iterator().next().get("{\"ID\":1}").toString())
        .isEqualTo("{\"ID\":1,\"NOTE\":\"text\",\"B\":\"AQ==\",\"N\":2}");
  }

  @Test
  void materialisesInsertUpdateDeleteAndTracksTransactions() throws Exception {
    Materialiser m = new Materialiser();
    m.apply(
        new RecordJson(
            record(0, "{\"ID\":1}", "c", null, "{\"ID\":1,\"NAME\":\"a\"}", "1.1.1", 100, 0, 2)));
    m.apply(
        new RecordJson(
            record(1, "{\"ID\":2}", "c", null, "{\"ID\":2,\"NAME\":\"b\"}", "1.1.1", 100, 1, 2)));
    m.apply(
        new RecordJson(
            record(
                2,
                "{\"ID\":1}",
                "u",
                "{\"ID\":1,\"NAME\":\"a\"}",
                "{\"ID\":1,\"NAME\":\"a2\"}",
                "1.1.2",
                110,
                0,
                1)));
    m.apply(
        new RecordJson(
            record(3, "{\"ID\":2}", "d", "{\"ID\":2,\"NAME\":\"b\"}", null, "1.1.3", 120, 0, 1)));
    m.apply(
        new RecordJson(record(4, "{\"ID\":2}", null, null, null, "1.1.3", 120, 0, 1))); // tombstone
    m.apply(
        new RecordJson(
            record(
                5,
                "{\"ID\":1}",
                "u",
                null,
                "{\"ID\":1,\"NAME\":\"a3\"}",
                "1.1.4",
                115,
                0,
                1))); // out of order
    assertThat(m.records()).isEqualTo(6);
    assertThat(m.tombstones()).isEqualTo(1);
    assertThat(m.tables()).containsOnlyKeys("APP.T");
    JsonNode row = m.tables().get("APP.T").get("{\"ID\":1}");
    assertThat(row.path("NAME").asText()).isEqualTo("a3");
    assertThat(m.tables().get("APP.T")).doesNotContainKey("{\"ID\":2}");
    assertThat(m.xids()).containsExactly("1.1.1", "1.1.2", "1.1.3", "1.1.4");
    assertThat(m.transactions().get("1.1.1").indices).containsExactly(0, 1);
    assertThat(m.transactions().get("1.1.1").opCopies).containsEntry("0:c", 1);
    assertThat(m.transactions().get("1.1.1").eventCount).isEqualTo(2);
    assertThat(m.maxCommitScn()).isEqualTo(120);
    assertThat(m.orderViolations()).hasSize(1);
  }

  @Test
  void schemaWrappedRecordsAreUnwrapped() throws Exception {
    String wrapped =
        "{\"schema\":{\"type\":\"struct\"},\"payload\":{\"op\":\"c\",\"before\":null,\"after\":{\"ID\":5},\"source\":{\"txId\":\"2.2.2\",\"commit_scn\":\"7\",\"scn\":\"6\",\"schema\":\"APP\",\"table\":\"T\"}}}";
    ConsumerRecord<String, String> r =
        new ConsumerRecord<>("t", 0, 0, "{\"schema\":{},\"payload\":{\"ID\":5}}", wrapped);
    RecordJson j = new RecordJson(r);
    assertThat(j.op).isEqualTo("c");
    assertThat(j.keyText()).isEqualTo("{\"ID\":5}");
    assertThat(j.after.path("ID").asInt()).isEqualTo(5);
    assertThat(j.commitScn).isEqualTo(7);
    assertThat(j.eventIndex).isNull();
  }

  @Test
  void normaliserAgreesOnNumbersTemporalsAndIntervals() throws Exception {
    Normaliser.Column num = new Normaliser.Column("N", Types.NUMERIC, "NUMBER", 10, 2);
    assertThat(Normaliser.fromRecord(M.readTree("10.50"), num)).isEqualTo("10.5");
    assertThat(
            Normaliser.fromRecord(
                M.readTree(
                    "{\"scale\":2,\"value\":\""
                        + java.util.Base64.getEncoder()
                            .encodeToString(new BigDecimal("12.34").unscaledValue().toByteArray())
                        + "\"}"),
                num))
        .isEqualTo("12.34");
    assertThat(Normaliser.number(new BigDecimal("0.000"))).isEqualTo("0");
    assertThat(Normaliser.number(new BigDecimal("1E+3"))).isEqualTo("1000");
    Normaliser.Column date = new Normaliser.Column("D", Types.TIMESTAMP, "DATE", 0, 0);
    assertThat(Normaliser.fromRecord(M.readTree("1772286359000"), date))
        .isEqualTo("2026-02-28T13:45:59");
    Normaliser.Column ts6 = new Normaliser.Column("T", Types.TIMESTAMP, "TIMESTAMP(6)", 0, 6);
    assertThat(Normaliser.fromRecord(M.readTree("1772286359123456"), ts6))
        .isEqualTo("2026-02-28T13:45:59.123456");
    Normaliser.Column ts9 = new Normaliser.Column("T", Types.TIMESTAMP, "TIMESTAMP(9)", 0, 9);
    assertThat(Normaliser.fromRecord(M.readTree("1772286359123456789"), ts9))
        .isEqualTo("2026-02-28T13:45:59.123456789");
    Normaliser.Column tz = new Normaliser.Column("Z", -101, "TIMESTAMP(6) WITH TIME ZONE", 0, 6);
    assertThat(Normaliser.fromRecord(M.readTree("\"2026-03-29T01:30:00.5+05:30\""), tz))
        .isEqualTo("2026-03-28T20:00:00.500Z");
    Normaliser.Column ym =
        new Normaliser.Column("I", Types.OTHER, "INTERVAL YEAR(4) TO MONTH", 0, 0);
    assertThat(Normaliser.intervalMicros("+12-03", ym.typeName()))
        .isEqualTo(Long.toString(Math.round(147 * Normaliser.MICROS_PER_MONTH)));
    assertThat(
            Normaliser.fromRecord(
                M.readTree(Long.toString(Math.round(147 * Normaliser.MICROS_PER_MONTH))), ym))
        .isEqualTo(Long.toString(Math.round(147 * Normaliser.MICROS_PER_MONTH)));
    Normaliser.Column ds =
        new Normaliser.Column("I", Types.OTHER, "INTERVAL DAY(5) TO SECOND(6)", 0, 6);
    assertThat(Normaliser.intervalMicros("+00005 04:03:02.123456", ds.typeName()))
        .isEqualTo(Long.toString(((5L * 24 + 4) * 3600 + 3 * 60 + 2) * 1_000_000L + 123456));
    Normaliser.Column raw = new Normaliser.Column("R", Types.VARBINARY, "RAW", 0, 0);
    assertThat(
            Normaliser.fromRecord(
                M.readTree(
                    "\""
                        + java.util.Base64.getEncoder()
                            .encodeToString(new byte[] {(byte) 0xde, (byte) 0xad})
                        + "\""),
                raw))
        .isEqualTo("dead");
    assertThat(Normaliser.fromRecord(M.readTree("null"), raw)).isNull();
  }

  @Test
  void reportCarriesVerdictAndHash() throws Exception {
    CheckReport r = new CheckReport();
    assertThat(r.exitCode()).isZero();
    r.inconclusive("x");
    assertThat(r.exitCode()).isEqualTo(CheckReport.EXIT_INCONCLUSIVE);
    r.fail("missing");
    assertThat(r.exitCode()).isEqualTo(CheckReport.EXIT_FAIL);
    String json = r.toJson();
    assertThat(json).contains("\"verdict\" : \"FAIL\"").contains("\"sha256\"");
    JsonNode n = M.readTree(json);
    assertThat(n.path("sha256").asText()).hasSize(64);
    assertThat(n.path("report").asText()).isEqualTo("oracle-cdc-correctness");
    assertThat(n.path("schemaVersion").asInt()).isEqualTo(1);
    assertThat(n.path("failures")).hasSize(1);
    assertThat(
            StateChecker.keyOf(
                new java.util.LinkedHashMap<>(java.util.Map.of("ID", "1", "X", "y")),
                List.of("ID")))
        .isEqualTo("ID=1;");
  }
}
