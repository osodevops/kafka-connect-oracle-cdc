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

/** See {@link ErrorCode#DICTIONARY_UNAVAILABLE}. */
public final class DictionaryUnavailableException extends OracleCdcException {
  private static final long serialVersionUID = 1L;

  public DictionaryUnavailableException(String message, String operatorAction) {
    super(ErrorCode.DICTIONARY_UNAVAILABLE, message, operatorAction);
  }

  public DictionaryUnavailableException(String message, String operatorAction, Throwable cause) {
    super(ErrorCode.DICTIONARY_UNAVAILABLE, message, operatorAction, cause);
  }

  /**
   * ADR-0027: in range mode LogMiner cannot use a dictionary from the redo (ORA-01371), so rows
   * written before a DDL the connector has not mined yet cannot be decoded.
   */
  public static DictionaryUnavailableException inRangeMode(long scn) {
    return new DictionaryUnavailableException(
        "Rows before SCN "
            + scn
            + " were written before a DDL the connector has not mined yet. Decoding them needs a"
            + " dictionary from the redo, which LogMiner cannot use from inside a pluggable"
            + " database (range mode, ORA-01371).",
        "Resnapshot the affected tables (oracle-cdc-admin resnapshot), or move the offset past the"
            + " DDL, in which case the table's rows in between are not delivered. Keep the"
            + " connector's lag short where DDL is frequent (ADR-0027).");
  }
}
