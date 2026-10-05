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
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.errors.TopologyException;
import sh.oso.connect.oracle.core.topology.DatabaseInfo;
import sh.oso.connect.oracle.core.topology.TopologyProbe;

/**
 * What every rule can see: the configuration, the catalog and the captured tables (resolved once),
 * plus the optional facts of the full rule set: the raw connector properties (keys outside the core
 * configuration, such as {@code exactly.once.support}), Kafka and worker facts, the internal topics
 * and the planned maximum downtime.
 */
public final class DoctorContext {

  /** DOC-10 and sizing when the operator states no maximum downtime. */
  public static final Duration DEFAULT_MAX_DOWNTIME = Duration.ofHours(24);

  private final CoreConfig config;
  private final DoctorCatalog catalog;
  private final List<Pattern> include;
  private final List<Pattern> exclude;
  private final String keyMissing;
  private DatabaseInfo database;
  private List<CapturedTable> tables;
  private Integer destId;
  private Map<String, String> connectorProperties = Map.of();
  private KafkaFacts kafka;
  private List<InternalTopic> internalTopics = List.of();
  private WorkerFacts worker;
  private Duration maxDowntime = DEFAULT_MAX_DOWNTIME;
  private Clock clock = Clock.system(ZoneOffset.UTC);

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

  /** The connector's properties as given, for keys the core configuration does not define. */
  public DoctorContext withConnectorProperties(Map<String, String> props) {
    this.connectorProperties = Map.copyOf(new LinkedHashMap<>(props));
    return this;
  }

  /** Broker facts for DOC-17 and DOC-18, with the internal topics the connector would use. */
  public DoctorContext withKafka(KafkaFacts facts, List<InternalTopic> topics) {
    this.kafka = facts;
    this.internalTopics = List.copyOf(topics);
    return this;
  }

  /** The internal topics without broker access (DOC-17 then reports what it could not check). */
  public DoctorContext withInternalTopics(List<InternalTopic> topics) {
    this.internalTopics = List.copyOf(topics);
    return this;
  }

  public DoctorContext withWorker(WorkerFacts facts) {
    this.worker = facts;
    return this;
  }

  public DoctorContext withMaxDowntime(Duration downtime) {
    this.maxDowntime = downtime;
    return this;
  }

  public DoctorContext withClock(Clock clock) {
    this.clock = clock;
    return this;
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

  public Map<String, String> connectorProperties() {
    return connectorProperties;
  }

  /** A connector property, or {@code dflt} when it is not set. */
  public String property(String key, String dflt) {
    String v = connectorProperties.get(key);
    return v == null || v.isBlank() ? dflt : v.trim();
  }

  /** Broker facts, or null when the doctor has no broker access. */
  public KafkaFacts kafka() {
    return kafka;
  }

  public List<InternalTopic> internalTopics() {
    return internalTopics;
  }

  /** Worker facts, or null when no Connect URL was given. */
  public WorkerFacts worker() {
    return worker;
  }

  public Duration maxDowntime() {
    return maxDowntime;
  }

  public Clock clock() {
    return clock;
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

  /**
   * The archive destination the engine would mine from (the configured one or the lowest valid
   * local one), or -1 when there is none; DOC-12 reports that case.
   */
  public int archiveDestId() throws SQLException {
    if (destId == null) {
      try {
        destId =
            new TopologyProbe(catalog, config.getString(CoreConfig.ARCHIVE_DESTINATION))
                .probe()
                .archiveDestId();
      } catch (TopologyException e) {
        destId = -1;
      }
    }
    return destId;
  }
}
