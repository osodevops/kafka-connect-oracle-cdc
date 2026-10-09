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

import java.util.Objects;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * ADR-0026: one redo thread's part of a position. {@code resume} is where that thread's redo is
 * read again after a restart (its redo byte address is only comparable within the thread); {@code
 * commit} and {@code commitKey} name the last commit on that thread the framework acknowledged, or
 * are both null when none was.
 */
public record ThreadMark(RedoRecordId resume, RedoRecordId commit, TxKey commitKey) {

  public ThreadMark {
    Objects.requireNonNull(resume, "resume");
    if ((commit == null) != (commitKey == null)) {
      throw new IllegalArgumentException("a thread's commit needs both its record and its key");
    }
  }

  public boolean hasCommit() {
    return commitKey != null;
  }
}
