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

import sh.oso.connect.oracle.core.decode.RowDecoder;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.schema.ColumnFilter;
import sh.oso.connect.oracle.core.schema.TableSchema;

/** Decodes one DML event; the default is {@link RowDecoder}, tests may substitute a stub. */
public interface ChangeDecoder {
  RowChange decode(MiningEvent.Dml dml, TableSchema schema);

  /**
   * SRC-SEL-2: decodes without the columns {@code excluded} names. {@link #rowDecoder()} drops them
   * before their literals are converted; a substitute decoder's result is projected afterwards.
   */
  default RowChange decode(MiningEvent.Dml dml, TableSchema schema, ColumnFilter excluded) {
    return excluded.project(decode(dml, schema));
  }

  static ChangeDecoder rowDecoder() {
    return new ChangeDecoder() {
      @Override
      public RowChange decode(MiningEvent.Dml dml, TableSchema schema) {
        return RowDecoder.decode(dml, schema);
      }

      @Override
      public RowChange decode(MiningEvent.Dml dml, TableSchema schema, ColumnFilter excluded) {
        return RowDecoder.decode(dml, schema, excluded);
      }
    };
  }
}
