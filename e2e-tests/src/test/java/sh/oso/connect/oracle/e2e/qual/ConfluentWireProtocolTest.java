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
package sh.oso.connect.oracle.e2e.qual;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

/** Tests public wire framing using our own data and Apache Avro, without Confluent software. */
class ConfluentWireProtocolTest {
  @Test
  void topLevelBytesAreRawAfterFramingWhileNestedBytesRetainTheirAvroLength() throws Exception {
    byte[] bytes = new byte[] {-1, 0, 1, 2};
    byte[] wire = new byte[] {0, 0, 0, 0, 1, -1, 0, 1, 2};
    assertThat((ByteBuffer) ConfluentFormatLab.avroDatum(Schema.create(Schema.Type.BYTES), wire))
        .isEqualTo(ByteBuffer.wrap(bytes));
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"OsoWireTest\","
                    + "\"fields\":[{\"name\":\"data\",\"type\":\"bytes\"}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("data", ByteBuffer.wrap(bytes));
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    output.write(new byte[] {0, 0, 0, 0, 2});
    var encoder = EncoderFactory.get().binaryEncoder(output, null);
    new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
    encoder.flush();
    GenericRecord decoded =
        (GenericRecord) ConfluentFormatLab.avroDatum(schema, output.toByteArray());
    assertThat(decoded.get("data")).isEqualTo(ByteBuffer.wrap(bytes));
  }
}
