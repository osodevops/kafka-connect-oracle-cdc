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

import java.util.Map;
import sh.oso.connect.oracle.core.model.RowChange;

/** Rough heap cost of a change, for the memory budget; exact enough to decide what to spill. */
final class SizeEstimate {

  private static final long BASE = 160;
  private static final long PER_ENTRY = 48;

  private SizeEstimate() {}

  static long of(RowChange c) {
    long n = BASE + (c.rowId() == null ? 0 : c.rowId().length() * 2L);
    n += of(c.before());
    n += of(c.after());
    return n;
  }

  private static long of(Map<String, Object> image) {
    if (image == null) {
      return 0;
    }
    long n = 32;
    for (Map.Entry<String, Object> e : image.entrySet()) {
      n += PER_ENTRY + e.getKey().length() * 2L;
      Object v = e.getValue();
      if (v instanceof CharSequence s) {
        n += s.length() * 2L;
      } else if (v instanceof byte[] b) {
        n += b.length;
      } else if (v != null) {
        n += 32;
      }
    }
    return n;
  }
}
