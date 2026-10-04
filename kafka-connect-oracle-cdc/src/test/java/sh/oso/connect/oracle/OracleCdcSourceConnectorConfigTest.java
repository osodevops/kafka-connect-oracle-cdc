package sh.oso.connect.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class OracleCdcSourceConnectorConfigTest {

  private static Map<String, String> minimal() {
    Map<String, String> p = new HashMap<>();
    p.put(OracleCdcSourceConnectorConfig.DATABASE_HOST, "oracle");
    p.put(OracleCdcSourceConnectorConfig.DATABASE_SERVICE, "FREE");
    p.put(OracleCdcSourceConnectorConfig.DATABASE_USER, "C##CDC");
    p.put(OracleCdcSourceConnectorConfig.DATABASE_PASSWORD, "secret");
    p.put(OracleCdcSourceConnectorConfig.TOPIC_PREFIX, "cdc");
    p.put(OracleCdcSourceConnectorConfig.TABLES_INCLUDE, "FREEPDB1\\.APP\\..*");
    return p;
  }

  @Test
  void everyKeyUsesTheCdcPrefix() {
    for (String key : OracleCdcSourceConnectorConfig.configDef().names()) {
      assertThat(key).startsWith("cdc.");
    }
  }

  @Test
  void everyKeyHasGroupDisplayNameAndDocumentation() {
    for (ConfigDef.ConfigKey key : OracleCdcSourceConnectorConfig.configDef().configKeys().values()) {
      assertThat(key.group).as("%s group", key.name).isNotBlank();
      assertThat(key.displayName).as("%s displayName", key.name).isNotBlank();
      assertThat(key.documentation).as("%s documentation", key.name).isNotBlank();
    }
  }

  @Test
  void minimalConfigParsesWithDefaults() {
    OracleCdcSourceConnectorConfig c = new OracleCdcSourceConnectorConfig(minimal());
    assertThat(c.getInt(OracleCdcSourceConnectorConfig.DATABASE_PORT)).isEqualTo(1521);
    assertThat(c.topicPrefix()).isEqualTo("cdc");
    assertThat(c.tablesInclude()).containsExactly("FREEPDB1\\.APP\\..*");
    assertThat(c.tablesExclude()).isEmpty();
    assertThat(c.getList(OracleCdcSourceConnectorConfig.DATABASE_PDBS)).isEmpty();
  }

  @Test
  void passwordIsNeverPrintedByToString() {
    OracleCdcSourceConnectorConfig c = new OracleCdcSourceConnectorConfig(minimal());
    assertThat(c.databasePassword().value()).isEqualTo("secret");
    assertThat(c.databasePassword().toString()).doesNotContain("secret");
  }

  @Test
  void requiresTopicPrefixAndTables() {
    Map<String, String> p = minimal();
    p.remove(OracleCdcSourceConnectorConfig.TOPIC_PREFIX);
    assertThatThrownBy(() -> new OracleCdcSourceConnectorConfig(p))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("cdc.topic.prefix");
    Map<String, String> q = minimal();
    q.remove(OracleCdcSourceConnectorConfig.TABLES_INCLUDE);
    assertThatThrownBy(() -> new OracleCdcSourceConnectorConfig(q))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("cdc.tables.include");
  }

  @Test
  void requiresHostOrUrlAndServiceOrSid() {
    Map<String, String> p = minimal();
    p.remove(OracleCdcSourceConnectorConfig.DATABASE_HOST);
    assertThatThrownBy(() -> new OracleCdcSourceConnectorConfig(p))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("cdc.database.url");
    Map<String, String> q = minimal();
    q.remove(OracleCdcSourceConnectorConfig.DATABASE_SERVICE);
    assertThatThrownBy(() -> new OracleCdcSourceConnectorConfig(q))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("cdc.database.sid");
    Map<String, String> r = minimal();
    r.remove(OracleCdcSourceConnectorConfig.DATABASE_HOST);
    r.remove(OracleCdcSourceConnectorConfig.DATABASE_SERVICE);
    r.put(OracleCdcSourceConnectorConfig.DATABASE_URL, "jdbc:oracle:thin:@//oracle:1521/FREE");
    assertThat(new OracleCdcSourceConnectorConfig(r).getString(OracleCdcSourceConnectorConfig.DATABASE_URL))
        .isNotBlank();
  }

  @Test
  void connectorAlwaysReturnsOneTaskConfig() {
    OracleCdcSourceConnector connector = new OracleCdcSourceConnector();
    connector.start(minimal());
    List<Map<String, String>> tasks = connector.taskConfigs(4);
    assertThat(tasks).hasSize(1);
    assertThat(tasks.get(0)).containsAllEntriesOf(minimal());
    assertThat(connector.taskClass()).isEqualTo(OracleCdcSourceTask.class);
    connector.stop();
  }
}
