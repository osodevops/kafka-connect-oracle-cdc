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
package sh.oso.connect.oracle.core.schema;

import java.util.Locale;

/** Oracle column data types the decoder knows, parsed from the dictionary's DATA_TYPE text. */
public enum OracleType {
  VARCHAR2,
  CHAR,
  NVARCHAR2,
  NCHAR,
  NUMBER,
  FLOAT,
  BINARY_FLOAT,
  BINARY_DOUBLE,
  DATE,
  TIMESTAMP,
  TIMESTAMP_TZ,
  TIMESTAMP_LTZ,
  INTERVAL_YM,
  INTERVAL_DS,
  RAW,
  LONG,
  LONG_RAW,
  CLOB,
  NCLOB,
  BLOB,
  BFILE,
  XMLTYPE,
  BOOLEAN,
  JSON,
  VECTOR,
  ROWID,
  UROWID,
  UNKNOWN;

  /** Maps DBA_TAB_COLS.DATA_TYPE such as {@code TIMESTAMP(6) WITH LOCAL TIME ZONE}. */
  public static OracleType fromDictionary(String dataType) {
    if (dataType == null) {
      return UNKNOWN;
    }
    String t = dataType.trim().toUpperCase(Locale.ROOT);
    if (t.startsWith("TIMESTAMP")) {
      if (t.contains("LOCAL TIME ZONE")) {
        return TIMESTAMP_LTZ;
      }
      return t.contains("TIME ZONE") ? TIMESTAMP_TZ : TIMESTAMP;
    }
    if (t.startsWith("INTERVAL YEAR")) {
      return INTERVAL_YM;
    }
    if (t.startsWith("INTERVAL DAY")) {
      return INTERVAL_DS;
    }
    if (t.startsWith("VECTOR")) {
      return VECTOR;
    }
    switch (t) {
      case "VARCHAR2":
      case "VARCHAR":
        return VARCHAR2;
      case "CHAR":
        return CHAR;
      case "NVARCHAR2":
        return NVARCHAR2;
      case "NCHAR":
        return NCHAR;
      case "NUMBER":
      case "INTEGER":
      case "DECIMAL":
        return NUMBER;
      case "FLOAT":
        return FLOAT;
      case "BINARY_FLOAT":
        return BINARY_FLOAT;
      case "BINARY_DOUBLE":
        return BINARY_DOUBLE;
      case "DATE":
        return DATE;
      case "RAW":
        return RAW;
      case "LONG":
        return LONG;
      case "LONG RAW":
        return LONG_RAW;
      case "CLOB":
        return CLOB;
      case "NCLOB":
        return NCLOB;
      case "BLOB":
        return BLOB;
      case "BFILE":
        return BFILE;
      case "XMLTYPE":
      case "SYS.XMLTYPE":
        return XMLTYPE;
      case "BOOLEAN":
        return BOOLEAN;
      case "JSON":
        return JSON;
      case "ROWID":
        return ROWID;
      case "UROWID":
        return UROWID;
      default:
        return UNKNOWN;
    }
  }

  /** LogMiner marks the whole row UNSUPPORTED for these on 23ai (reference/sql-redo-shapes.md). */
  public boolean unsupportedByLogMiner() {
    return this == BOOLEAN || this == JSON || this == VECTOR || this == BFILE || this == UNKNOWN;
  }

  public boolean isLob() {
    return this == CLOB || this == NCLOB || this == BLOB || this == XMLTYPE;
  }
}
