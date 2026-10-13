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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.OracleType;

class ConfluentTypesTest {
  @Test
  void aNegativeScaleCannotWrapAnIntegerBeyondTheSelectedWidth() {
    ConfluentTypes types =
        new ConfluentTypes(
            ConfluentTypes.NumericMapping.BEST_FIT_OR_DECIMAL,
            127,
            ConfluentTypes.DateMapping.DATE);
    for (int precision : List.of(2, 3, 9, 18)) {
      ColumnSpec column =
          new ColumnSpec("N", 1, OracleType.NUMBER, "NUMBER", 0, precision, -2, true);
      BigDecimal value = new BigDecimal("9".repeat(precision) + "00");
      assertThatThrownBy(() -> types.value(column, types.schema(column, false), value))
          .isInstanceOf(DecodeException.class)
          .hasMessageContaining("integer");
    }
  }
}
