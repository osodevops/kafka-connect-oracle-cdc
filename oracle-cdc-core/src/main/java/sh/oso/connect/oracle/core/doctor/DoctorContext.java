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
import java.util.Locale;
import java.util.regex.Pattern;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.topology.DatabaseInfo;

/**
 * What every rule can see: the configuration, the catalog and the captured tables (resolved once).
 */
public final class DoctorContext {

  private final CoreConfig config;
  private final DoctorCatalog catalog;
  private final List<Pattern> include;
  private final List<Pattern> exclude;
  private final String keyMissing;
  private DatabaseInfo database;
  private List<CapturedTable> tables;

  public DoctorContext(
      CoreConfig config,
      DoctorCatalog catalog,
      List<String> include,
      List<String> exclude,
      String keyMissing) {
    this.config = config;
    this.catalog = catalog;
    this.include = include.stream().map(p -> Pattern.compile(p, Pattern.CASE_INSENSITIVE)).toList();
    this.exclude = exclude.stream().map(p -> Pattern.compile(p, Pattern.CASE_INSENSITIVE)).toList();
    this.keyMissing = keyMissing == null ? "fail" : keyMissing.toLowerCase(Locale.ROOT);
  }

  public CoreConfig config() {
    return config;
  }

  public DoctorCatalog catalog() {
    return catalog;
  }

  public String keyMissing() {
    return keyMissing;
  }

  public DatabaseInfo database() throws SQLException {
    if (database == null) {
      database = catalog.database();
    }
    return database;
  }

  public List<CapturedTable> tables() throws SQLException {
    if (tables == null) {
      tables = catalog.capturedTables(include, exclude, config.pdbs());
    }
    return tables;
  }
}
