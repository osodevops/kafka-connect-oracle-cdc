package sh.oso.connect.oracle;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
          "tasks.max={} requested but this connector always runs one task; the extra tasks are ignored",
          maxTasks);
    }
    return List.of(props);
  }

  @Override
  public void stop() {
    LOG.info("OSO CDC Connector for Oracle Database stopped");
  }

  @Override
  public ConfigDef config() {
    return OracleCdcSourceConnectorConfig.configDef();
  }

  @Override
  public String version() {
    return Version.VERSION;
  }
}
