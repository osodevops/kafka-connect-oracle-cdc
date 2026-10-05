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
package sh.oso.connect.oracle.core.snapshot;

import java.util.List;

/**
 * A chunk of a table: from {@code lower} (inclusive) to {@code upper} (exclusive), each an encoded
 * key tuple or ROWID ({@link BoundCodec}); null means open at that end.
 */
public record ChunkRange(List<String> lower, List<String> upper) {

  public static final ChunkRange ALL = new ChunkRange(null, null);

  public ChunkRange {
    lower = lower == null ? null : List.copyOf(lower);
    upper = upper == null ? null : List.copyOf(upper);
  }

  /** The table's last chunk: nothing follows it. */
  public boolean last() {
    return upper == null;
  }
}
