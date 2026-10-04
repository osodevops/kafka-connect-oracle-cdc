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

import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;
import sh.oso.connect.oracle.core.topology.CatalogSource;

/** Dictionary facts the rules need on top of {@link CatalogSource}; faked in unit tests. */
public interface DoctorCatalog extends CatalogSource {

  /** Tables matching the include patterns and not the exclude patterns, across the given PDBs. */
  List<CapturedTable> capturedTables(
      List<Pattern> include, List<Pattern> exclude, List<String> pdbs) throws SQLException;

  /** System privileges and roles granted (directly or through roles) to the connected user. */
  List<String> grantedPrivileges() throws SQLException;

  /** Fixed views the user cannot select from, out of the ones the engine needs. */
  List<String> inaccessibleViews() throws SQLException;

  /** The connected user's CONTAINER_DATA attribute covers all containers. */
  boolean containerDataAll() throws SQLException;

  /** Whether the connected user is a common user (CDB only). */
  boolean commonUser() throws SQLException;
}
