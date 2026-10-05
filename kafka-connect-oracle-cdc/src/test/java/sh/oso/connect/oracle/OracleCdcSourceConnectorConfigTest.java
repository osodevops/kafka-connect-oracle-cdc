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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.schema.KeySelector;

public class OracleCdcSourceConnectorConfigTest {

  public static Map<String, String> minimal() {
    Map<String, String> p = new HashMap<>();
    p.put(CoreConfig.DATABASE_HOST, "oracle");
    p.put(CoreConfig.DATABASE_SERVICE, "FREE");
    p.put(CoreConfig.DATABASE_USER, "c##cdc");
    p.put(CoreConfig.DATABASE_PASSWORD, "cdc");
    p.put(CoreConfig.DATABASE_PDBS, "FREEPDB1");
    p.put(OracleCdcSourceConnectorConfig.TOPIC_PREFIX, "cdc");
    p.put(OracleCdcSourceConnectorConfig.TABLES_INCLUDE, "FREEPDB1\\.APP\\..*");
    return p;
  }

  @Test
  void composesTheCoreDefinitionWithTheConnectorKeys() {
    var def = OracleCdcSourceConnectorConfig.configDef();
    assertThat(def.names())
        .contains(
            CoreConfig.DATABASE_HOST,
            CoreConfig.MINING_TARGET_LATENCY_MS,
            OracleCdcSourceConnectorConfig.TOPIC_PREFIX,
            OracleCdcSourceConnectorConfig.TABLES_INCLUDE,
            OracleCdcSourceConnectorConfig.KEY_MISSING,
            OracleCdcSourceConnectorConfig.DECIMAL_MODE,
            OracleCdcSourceConnectorConfig.POLL_MAX_RECORDS);
    assertThat(def.names()).allMatch(n -> n.startsWith("cdc."));
    assertThat(def.groups()).contains("Database", "Topics", "Record format", "Task");
  }

  @Test
  void defaultsAndAccessors() {
    OracleCdcSourceConnectorConfig c = new OracleCdcSourceConnectorConfig(minimal());
    assertThat(c.topicPrefix()).isEqualTo("cdc");
    assertThat(c.topicTemplate(true)).isEqualTo("${prefix}.${pdb}.${schema}.${table}");
    assertThat(c.topicTemplate(false)).isEqualTo("${prefix}.${schema}.${table}");
    assertThat(c.tablesInclude()).containsExactly("FREEPDB1\\.APP\\..*");
    assertThat(c.tablesExclude()).isEmpty();
    assertThat(c.usersExclude()).isEmpty();
    assertThat(c.keyMissing()).isEqualTo(KeySelector.MissingKeyPolicy.FAIL);
    assertThat(c.keyOverrides()).isEmpty();
    assertThat(c.tombstonesOnDelete()).isTrue();
    assertThat(c.decimalMode()).isEqualTo(OracleCdcSourceConnectorConfig.DecimalMode.PRECISE);
    assertThat(c.temporalMode()).isEqualTo(OracleCdcSourceConnectorConfig.TemporalMode.ADAPTIVE);
    assertThat(c.pollMaxRecords()).isEqualTo(2000);
    assertThat(c.pollLingerMs()).isEqualTo(50);
    assertThat(c.shutdownTimeoutMs()).isEqualTo(30_000);
    assertThat(c.core().pdbs()).containsExactly("FREEPDB1");
    assertThat(c.databasePassword().value()).isEqualTo("cdc");
    assertThat(c.rawProperties()).containsEntry(CoreConfig.DATABASE_USER, "c##cdc");
  }

  @Test
  void keyOverridesAndValidation() {
    Map<String, String> p = minimal();
    p.put(
        OracleCdcSourceConnectorConfig.KEY_COLUMNS, "app.orders:id, code; FREEPDB1.APP.ITEMS:sku");
    p.put(OracleCdcSourceConnectorConfig.KEY_MISSING, "RowId");
    p.put(OracleCdcSourceConnectorConfig.TOPIC_TEMPLATE, "${prefix}-${table}");
    OracleCdcSourceConnectorConfig c = new OracleCdcSourceConnectorConfig(p);
    assertThat(c.keyOverrides())
        .containsEntry("APP.ORDERS", List.of("ID", "CODE"))
        .containsEntry("FREEPDB1.APP.ITEMS", List.of("SKU"));
    assertThat(c.keyMissing()).isEqualTo(KeySelector.MissingKeyPolicy.ROWID);
    assertThat(c.topicTemplate(true)).isEqualTo("${prefix}-${table}");
    p.put(OracleCdcSourceConnectorConfig.KEY_COLUMNS, "broken");
    assertThatThrownBy(() -> new OracleCdcSourceConnectorConfig(p))
        .isInstanceOf(ConfigException.class);
    Map<String, String> bad = minimal();
    bad.put(OracleCdcSourceConnectorConfig.KEY_MISSING, "maybe");
    assertThatThrownBy(() -> new OracleCdcSourceConnectorConfig(bad))
        .isInstanceOf(ConfigException.class);
    Map<String, String> noPrefix = minimal();
    noPrefix.remove(OracleCdcSourceConnectorConfig.TOPIC_PREFIX);
    assertThatThrownBy(() -> new OracleCdcSourceConnectorConfig(noPrefix))
        .isInstanceOf(ConfigException.class);
  }

  @Test
  void internalTopicsAndBrokerAccess() {
    OracleCdcSourceConnectorConfig c = new OracleCdcSourceConnectorConfig(minimal());
    assertThat(c.opsTopic()).isEqualTo("cdc.cdc.ops");
    assertThat(c.signalsTopic()).isEqualTo("cdc.cdc.signals");
    assertThat(c.schemaTopic()).isEqualTo("cdc.cdc.schema");
    assertThat(c.transactionsTopicEnabled()).isFalse();
    assertThat(c.transactionsTopic()).isEqualTo("cdc.cdc.transactions");
    assertThat(c.kafkaBootstrapServers()).isNull();
    assertThat(c.kafkaClientProperties()).isEmpty();
    assertThat(c.internalTopicReplication()).isEqualTo((short) -1);
    Map<String, String> p = minimal();
    p.put(OracleCdcSourceConnectorConfig.KAFKA_BOOTSTRAP_SERVERS, " ");
    assertThat(new OracleCdcSourceConnectorConfig(p).kafkaBootstrapServers()).isNull();
    p.put(OracleCdcSourceConnectorConfig.KAFKA_BOOTSTRAP_SERVERS, "kafka:9092");
    p.put("cdc.kafka.security.protocol", "SASL_SSL");
    p.put("cdc.kafka.sasl.mechanism", "SCRAM-SHA-512");
    p.put(OracleCdcSourceConnectorConfig.INTERNAL_TOPIC_REPLICATION, "3");
    OracleCdcSourceConnectorConfig k = new OracleCdcSourceConnectorConfig(p);
    assertThat(k.kafkaBootstrapServers()).isEqualTo("kafka:9092");
    assertThat(k.kafkaClientProperties())
        .containsEntry("security.protocol", "SASL_SSL")
        .containsEntry("sasl.mechanism", "SCRAM-SHA-512")
        .containsEntry("bootstrap.servers", "kafka:9092");
    assertThat(k.internalTopicReplication()).isEqualTo((short) 3);
  }

  @Test
  void columnExclusionsCompileWithTheTableCaseRulesAndSpareKeyOverrides() {
    Map<String, String> p = minimal();
    assertThat(new OracleCdcSourceConnectorConfig(p).columnFilter().isEmpty()).isTrue();
    p.put(
        OracleCdcSourceConnectorConfig.COLUMNS_EXCLUDE,
        "freepdb1\\.app\\.customers\\.ssn, .*\\.DOB");
    OracleCdcSourceConnectorConfig c = new OracleCdcSourceConnectorConfig(p);
    var customers = new sh.oso.connect.oracle.core.model.TableId("FREEPDB1", "APP", "CUSTOMERS");
    assertThat(c.columnFilter().excludes(customers, "SSN")).isTrue();
    assertThat(c.columnFilter().excludes(customers, "DOB")).isTrue();
    assertThat(c.columnFilter().excludes(customers, "NAME")).isFalse();
    assertThat(c.columnFilter()).as("one filter shared by every user").isSameAs(c.columnFilter());

    p.put(OracleCdcSourceConnectorConfig.TABLES_CASE_SENSITIVE, "true");
    assertThat(new OracleCdcSourceConnectorConfig(p).columnFilter().excludes(customers, "SSN"))
        .as("case-sensitive like the table patterns")
        .isFalse();
  }

  @Test
  void anInvalidColumnPatternOrAnExcludedKeyOverrideIsAConfigurationError() {
    Map<String, String> p = minimal();
    p.put(OracleCdcSourceConnectorConfig.COLUMNS_EXCLUDE, "APP\\.(SSN");
    var errors =
        OracleCdcSourceConnectorConfig.configDef().validate(p).stream()
            .filter(v -> v.name().equals(OracleCdcSourceConnectorConfig.COLUMNS_EXCLUDE))
            .findFirst()
            .orElseThrow()
            .errorMessages();
    assertThat(errors).singleElement().asString().contains("not a regular expression");
    assertThatThrownBy(() -> new OracleCdcSourceConnectorConfig(p))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("not a regular expression");

    Map<String, String> q = minimal();
    q.put(OracleCdcSourceConnectorConfig.KEY_COLUMNS, "FREEPDB1.APP.CUSTOMERS:ID,SSN");
    q.put(OracleCdcSourceConnectorConfig.COLUMNS_EXCLUDE, ".*\\.SSN");
    assertThatThrownBy(() -> new OracleCdcSourceConnectorConfig(q))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(OracleCdcSourceConnectorConfig.COLUMNS_EXCLUDE)
        .hasMessageContaining("FREEPDB1.APP.CUSTOMERS.SSN")
        .hasMessageContaining("key column cannot be excluded");
  }
}
