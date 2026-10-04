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
package sh.oso.connect.oracle.core.topology;

/** V$DATABASE and V$INSTANCE facts the engine keys decisions on (PRD-00 CORE-CONN-5). */
public record DatabaseInfo(
    long dbid,
    String name,
    boolean cdb,
    String logMode,
    String openMode,
    String databaseRole,
    String versionFull,
    long resetlogsChangeScn,
    boolean supplementalLogDataMin,
    String platformName) {

  public boolean archivelog() {
    return "ARCHIVELOG".equals(logMode);
  }

  public boolean primary() {
    return "PRIMARY".equals(databaseRole);
  }

  /** Identity that distinguishes this database and incarnation from any other. */
  public String identity() {
    return dbid + "@" + resetlogsChangeScn;
  }
}
