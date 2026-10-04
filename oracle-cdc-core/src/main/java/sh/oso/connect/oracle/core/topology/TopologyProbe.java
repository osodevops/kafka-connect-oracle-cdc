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

import java.sql.SQLException;
import java.util.List;
import sh.oso.connect.oracle.core.errors.TopologyException;

/** Builds a {@link Topology} and chooses the archive destination (PRD-00 CORE-LOG-1). */
public final class TopologyProbe {

  private final CatalogSource catalog;
  private final String configuredDestination;

  public TopologyProbe(CatalogSource catalog, String configuredDestination) {
    this.catalog = catalog;
    this.configuredDestination = configuredDestination;
  }

  public Topology probe() throws SQLException {
    DatabaseInfo db = catalog.database();
    List<ThreadInfo> threads = catalog.threads();
    List<PdbInfo> pdbs = db.cdb() ? catalog.pdbs() : List.of();
    return new Topology(db, threads, pdbs, chooseDestination(catalog.archiveDestinations()));
  }

  /** The configured destination by name, or the lowest valid local one. */
  int chooseDestination(List<ArchiveDestination> dests) {
    if (configuredDestination != null && !configuredDestination.isBlank()) {
      return dests.stream()
          .filter(d -> configuredDestination.equalsIgnoreCase(d.name()))
          .findFirst()
          .map(ArchiveDestination::destId)
          .orElseThrow(
              () ->
                  new TopologyException(
                      "Archive destination "
                          + configuredDestination
                          + " is not an active destination.",
                      "Set cdc.archive.destination to one of "
                          + dests.stream().map(ArchiveDestination::name).toList()
                          + " or leave it unset."));
    }
    return dests.stream()
        .filter(ArchiveDestination::validLocal)
        .mapToInt(ArchiveDestination::destId)
        .min()
        .orElseThrow(
            () ->
                new TopologyException(
                    "No valid local archive destination found.",
                    "Enable ARCHIVELOG mode with a local LOG_ARCHIVE_DEST_n and check"
                        + " V$ARCHIVE_DEST_STATUS."));
  }
}
