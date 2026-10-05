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
package sh.oso.connect.oracle;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.oracle.validation.ConnectorValidator;

/**
 * OSO CDC Connector for Oracle Database. Captures row changes from Oracle Database redo through
 * LogMiner and writes them to Kafka. Always runs exactly one task: LogMiner sessions over the same
 * redo multiply source database load, so parallelism comes from pipelining inside the task, not
 * from extra tasks (PRD-01 SRC-LC-2).
 */
public class OracleCdcSourceConnector extends SourceConnector {

  private static final Logger LOG = LoggerFactory.getLogger(OracleCdcSourceConnector.class);

  private Map<String, String> props;

  @Override
  public void start(Map<String, String> props) {
    // Fail fast on invalid settings before any task is scheduled.
    new OracleCdcSourceConnectorConfig(props);
    this.props = Map.copyOf(props);
    LOG.info("OSO CDC Connector for Oracle Database {} starting", Version.VERSION);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return OracleCdcSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    if (maxTasks > 1) {
      LOG.warn(
          "tasks.max={} requested but this connector always runs one task; the extra tasks are"
              + " ignored",
          maxTasks);
    }
    return List.of(props);
  }

  @Override
  public void stop() {
    LOG.info("OSO CDC Connector for Oracle Database stopped");
  }

  /** SRC-LC-1: field-level errors first, then the doctor's fast rules against the database. */
  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    Config config = super.validate(connectorConfigs);
    boolean clean = config.configValues().stream().allMatch(v -> v.errorMessages().isEmpty());
    return clean ? ConnectorValidator.validate(connectorConfigs, config) : config;
  }

  @Override
  public ConfigDef config() {
    return OracleCdcSourceConnectorConfig.configDef();
  }

  @Override
  public String version() {
    return Version.VERSION;
  }

  /** SRC-EOS-1: the task ends Kafka transactions only at Oracle commit boundaries. */
  @Override
  public org.apache.kafka.connect.source.ExactlyOnceSupport exactlyOnceSupport(
      Map<String, String> connectorConfig) {
    return org.apache.kafka.connect.source.ExactlyOnceSupport.SUPPORTED;
  }

  @Override
  public org.apache.kafka.connect.source.ConnectorTransactionBoundaries
      canDefineTransactionBoundaries(Map<String, String> connectorConfig) {
    return org.apache.kafka.connect.source.ConnectorTransactionBoundaries.SUPPORTED;
  }
}
