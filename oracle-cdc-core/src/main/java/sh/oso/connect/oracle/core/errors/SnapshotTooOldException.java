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
package sh.oso.connect.oracle.core.errors;

/** PRD-02 SNAP-4: undo for a snapshot chunk's SCN was gone even at the smallest chunk size. */
public final class SnapshotTooOldException extends OracleCdcException {
  private static final long serialVersionUID = 1L;

  public SnapshotTooOldException(String message, String operatorAction, Throwable cause) {
    super(ErrorCode.SNAPSHOT_TOO_OLD, message, operatorAction, cause);
  }
}
