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
package sh.oso.connect.oracle.core.buffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

class RowChangeCodecTest {

  static final TableId T = new TableId("FREEPDB1", "APP", "ORDERS");
  static final TxKey TX = new TxKey(3, new Xid(7, 21, 9001));

  @Test
  void roundTripsEveryTypeTheDecoderProduces() throws Exception {
    Map<String, Object> after = new LinkedHashMap<>();
    after.put("S", "héllo 🙂");
    after.put("EMPTY", "");
    after.put("NUL", null);
    after.put("N", new BigDecimal("-12345.678900"));
    after.put("BIG", new BigDecimal("1E+30"));
    after.put("F", 1.5f);
    after.put("D", -2.25d);
    after.put("RAW", new byte[] {0, 1, (byte) 0xff});
    after.put("DT", LocalDateTime.of(2026, 3, 29, 1, 30, 0, 123_456_789));
    after.put("TZ", OffsetDateTime.of(2026, 10, 5, 7, 0, 1, 5, ZoneOffset.ofHoursMinutes(5, 30)));
    after.put("INST", Instant.ofEpochSecond(1_700_000_000L, 42));
    after.put("DS", Duration.ofDays(3).plusNanos(7));
    after.put("YM", Period.of(2, 11, 0));
    after.put("L", 9_000_000_000L);
    after.put("I", 7);
    after.put("B", true);
    after.put("LD", LocalDate.of(1999, 12, 31));
    after.put("BI", new BigInteger("123456789012345678901234567890"));
    after.put("SH", (short) 3);
    after.put("BY", (byte) -1);
    Map<String, Object> before = new LinkedHashMap<>();
    before.put("S", "old");
    RowChange c =
        new RowChange(
            T,
            Operation.UPDATE,
            before,
            after,
            true,
            "AAAS6cAAHAAAAFdAAA",
            new RedoRecordId(123456789L, " 0x000123.0000abcd.0010 ", 2),
            TX,
            Instant.ofEpochSecond(1_600_000_000L, 999));
    RowChange back = RowChangeCodec.decode(RowChangeCodec.encode(c));
    assertThat(back.table()).isEqualTo(T);
    assertThat(back.op()).isEqualTo(Operation.UPDATE);
    assertThat(back.partial()).isTrue();
    assertThat(back.rowId()).isEqualTo(c.rowId());
    assertThat(back.id()).isEqualTo(c.id());
    assertThat(back.tx()).isEqualTo(TX);
    assertThat(back.timestamp()).isEqualTo(c.timestamp());
    assertThat(back.before()).containsExactlyEntriesOf(before);
    assertThat(back.after().keySet()).containsExactlyElementsOf(after.keySet());
    for (Map.Entry<String, Object> e : after.entrySet()) {
      Object v = back.after().get(e.getKey());
      if (e.getValue() instanceof byte[] b) {
        assertThat((byte[]) v).containsExactly(b);
      } else {
        assertThat(v).as(e.getKey()).isEqualTo(e.getValue());
        if (e.getValue() != null) {
          assertThat(v.getClass()).as(e.getKey()).isEqualTo(e.getValue().getClass());
        }
      }
    }
  }

  @Test
  void nullImagesAndNullOptionalFieldsSurvive() throws Exception {
    RowChange c =
        new RowChange(
            T,
            Operation.DELETE,
            Map.of("ID", BigDecimal.ONE),
            null,
            false,
            null,
            new RedoRecordId(5, "x", 0),
            TX,
            null);
    RowChange back = RowChangeCodec.decode(RowChangeCodec.encode(c));
    assertThat(back.after()).isNull();
    assertThat(back.rowId()).isNull();
    assertThat(back.timestamp()).isNull();
    assertThat(back.before()).containsEntry("ID", BigDecimal.ONE);
  }

  @Test
  void refusesAValueTypeItCannotReproduce() {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    assertThatThrownBy(
            () -> RowChangeCodec.writeValue(new DataOutputStream(bytes), new StringBuilder("x")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("StringBuilder");
  }

  @Test
  void rejectsAnUnknownTag() {
    assertThatThrownBy(
            () ->
                RowChangeCodec.readValue(
                    new DataInputStream(new java.io.ByteArrayInputStream(new byte[] {99}))))
        .isInstanceOf(java.io.IOException.class);
  }

  @Property(tries = 200)
  void anyStringAndDecimalRoundTrip(
      @ForAll("values") Map<String, Object> image, @ForAll Operation op) throws Exception {
    RowChange c =
        new RowChange(T, op, image, image, false, "r", new RedoRecordId(1, "a", 0), TX, null);
    RowChange back = RowChangeCodec.decode(RowChangeCodec.encode(c));
    assertThat(back.after()).containsExactlyEntriesOf(image);
    assertThat(back.before()).containsExactlyEntriesOf(image);
  }

  @Provide
  Arbitrary<Map<String, Object>> values() {
    Arbitrary<Object> value =
        Arbitraries.oneOf(
            List.of(
                Arbitraries.strings().ofMaxLength(40).map(s -> (Object) s),
                Arbitraries.bigDecimals().map(d -> (Object) d),
                Arbitraries.longs().map(l -> (Object) l),
                Arbitraries.just(null)));
    return Arbitraries.maps(Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8), value)
        .ofMaxSize(6)
        .map(LinkedHashMap::new);
  }

  static List<Object> bytesList(byte[] b) {
    return Arrays.asList((Object) b);
  }
}
