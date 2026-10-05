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
package sh.oso.connect.oracle.core.model;

/**
 * V$LOGMNR_CONTENTS OPERATION_CODE values the engine understands. The observed subset is recorded
 * in reference/operation-codes.md; the rest are Oracle's documented codes.
 */
public enum Operation {
  INTERNAL(0),
  INSERT(1),
  DELETE(2),
  UPDATE(3),
  DDL(5),
  START(6),
  COMMIT(7),
  SELECT_LOB_LOCATOR(9),
  LOB_WRITE(10),
  LOB_TRIM(11),
  SELECT_FOR_UPDATE(25),
  /** 29 on 23ai (reference/lob-redo-shapes.md), not the documented 28. */
  LOB_ERASE(29),
  MISSING_SCN(34),
  ROLLBACK(36),
  UNSUPPORTED(255),
  UNKNOWN(-1);

  private final int code;

  Operation(int code) {
    this.code = code;
  }

  public int code() {
    return code;
  }

  public static Operation fromCode(int code) {
    for (Operation op : values()) {
      if (op.code == code) {
        return op;
      }
    }
    return UNKNOWN;
  }

  /** Row-level changes to a captured object, including LOB fragments. */
  public boolean isRowChange() {
    return this == INSERT
        || this == UPDATE
        || this == DELETE
        || this == SELECT_LOB_LOCATOR
        || this == LOB_WRITE
        || this == LOB_TRIM
        || this == LOB_ERASE;
  }

  /** LOB rows: a PL/SQL block on the row's locator, folded into its change (CORE-DEC-6). */
  public boolean isLobOp() {
    return this == SELECT_LOB_LOCATOR || this == LOB_WRITE || this == LOB_TRIM || this == LOB_ERASE;
  }

  /** The operation of the undo row that reverses this one: INSERT and DELETE swap. */
  public Operation inverse() {
    if (this == INSERT) {
      return DELETE;
    }
    if (this == DELETE) {
      return INSERT;
    }
    return this;
  }

  public boolean isTransactionControl() {
    return this == START || this == COMMIT || this == ROLLBACK;
  }
}
