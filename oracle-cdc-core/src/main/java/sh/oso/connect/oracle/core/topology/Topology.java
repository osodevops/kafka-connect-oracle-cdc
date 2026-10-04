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

import java.util.List;
import java.util.stream.Collectors;

/** Snapshot of the database shape; a changed fingerprint triggers a controlled re-plan. */
public record Topology(
    DatabaseInfo database, List<ThreadInfo> threads, List<PdbInfo> pdbs, int archiveDestId) {

  public boolean rac() {
    return threads.stream().filter(ThreadInfo::enabled).count() > 1;
  }

  public String fingerprint() {
    return database.identity()
        + "|threads="
        + threads.stream()
            .filter(ThreadInfo::enabled)
            .map(t -> String.valueOf(t.thread()))
            .collect(Collectors.joining(","))
        + "|pdbs="
        + pdbs.stream()
            .filter(PdbInfo::readWrite)
            .map(PdbInfo::name)
            .collect(Collectors.joining(","))
        + "|dest="
        + archiveDestId
        + "|role="
        + database.databaseRole();
  }
}
