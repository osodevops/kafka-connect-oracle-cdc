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
package sh.oso.connect.oracle.core.doctor;

/**
 * One doctor finding: rule id (DOC-n), severity, plain-language message and the SQL that fixes it.
 */
public record Finding(String rule, Severity severity, String message, String fixSql) {
  public static Finding blocking(String rule, String message, String fixSql) {
    return new Finding(rule, Severity.BLOCKING, message, fixSql);
  }

  public static Finding warning(String rule, String message, String fixSql) {
    return new Finding(rule, Severity.WARNING, message, fixSql);
  }

  public static Finding info(String rule, String message) {
    return new Finding(rule, Severity.INFO, message, null);
  }
}
