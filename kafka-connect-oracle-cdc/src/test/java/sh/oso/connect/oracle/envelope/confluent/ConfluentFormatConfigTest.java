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
package sh.oso.connect.oracle.envelope.confluent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfigTest;

class ConfluentFormatConfigTest {
  static OracleCdcSourceConnectorConfig config(Map<String, String> overrides) {
    Map<String, String> props = OracleCdcSourceConnectorConfigTest.minimal();
    props.put(OracleCdcSourceConnectorConfig.OUTPUT_FORMAT, "confluent");
    props.putAll(overrides);
    return new OracleCdcSourceConnectorConfig(props);
  }

  @Test
  void defaultsMatchSettingsAndTombstoneDefaultDependsOnFormat() {
    assertThat(config(Map.of()).confluentSettings()).isEqualTo(ConfluentSettings.defaults(false));
    assertThat(
            new OracleCdcSourceConnectorConfig(OracleCdcSourceConnectorConfigTest.minimal())
                .tombstonesOnDelete())
        .isTrue();
    assertThat(
            config(Map.of(OracleCdcSourceConnectorConfig.TOMBSTONES_ON_DELETE, "true"))
                .confluentSettings())
        .isEqualTo(ConfluentSettings.defaults(true));
  }

  @Test
  void rejectsMalformedPlacementBeforePathsDuplicateFieldsAndDuplicateOperations() {
    for (String bad : java.util.List.of("header:", ".name", "name.", "a..b")) {
      assertThatThrownBy(() -> config(Map.of(ConfluentFormatConfig.SCN_FIELD, bad)))
          .isInstanceOf(ConfigException.class);
    }
    for (String bad : java.util.List.of("header:before", "meta.before", "table")) {
      assertThatThrownBy(() -> config(Map.of(ConfluentFormatConfig.BEFORE_STATE_FIELD, bad)))
          .isInstanceOf(ConfigException.class);
    }
    assertThatThrownBy(() -> config(Map.of(ConfluentFormatConfig.SCN_FIELD, "table")))
        .isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> config(Map.of(ConfluentFormatConfig.OP_INSERT, "R")))
        .isInstanceOf(ConfigException.class);
  }

  @Test
  void topicModeRequiresConfluentCompleteValuesAndAColumnTemplate() {
    for (Map<String, String> bad :
        java.util.List.of(
            Map.of(
                sh.oso.connect.oracle.core.config.CoreConfig.LOB_MODE,
                "topic",
                OracleCdcSourceConnectorConfig.OUTPUT_FORMAT,
                "debezium"),
            Map.of(
                sh.oso.connect.oracle.core.config.CoreConfig.LOB_MODE,
                "topic",
                sh.oso.connect.oracle.core.config.CoreConfig.LOB_OVERSIZE_ACTION,
                "placeholder"),
            Map.of(
                sh.oso.connect.oracle.core.config.CoreConfig.LOB_MODE,
                "topic",
                OracleCdcSourceConnectorConfig.LOB_TOPIC_TEMPLATE,
                "${table}"),
            Map.of(
                sh.oso.connect.oracle.core.config.CoreConfig.LOB_MODE,
                "topic",
                OracleCdcSourceConnectorConfig.LOB_TOPIC_TEMPLATE,
                "${unknown}.${column}"))) {
      assertThatThrownBy(() -> config(bad)).isInstanceOf(ConfigException.class);
    }
    assertThat(
            config(Map.of(sh.oso.connect.oracle.core.config.CoreConfig.LOB_MODE, "topic"))
                .lobTopicTemplate(true))
        .isEqualTo("${pdb}.${schema}.${table}.${column}");
  }

  @Test
  void validatesNumericMappingScaleAndDateMapping() {
    assertThat(
            config(
                    Map.of(
                        ConfluentFormatConfig.NUMERIC_MAPPING,
                        "BEST_FIT_OR_STRING",
                        ConfluentFormatConfig.NUMERIC_DEFAULT_SCALE,
                        "2",
                        ConfluentFormatConfig.DATE_MAPPING,
                        "TIMESTAMP"))
                .confluentSettings()
                .numericMapping())
        .isEqualTo(ConfluentTypes.NumericMapping.BEST_FIT_OR_STRING);
    for (Map<String, String> bad :
        java.util.List.of(
            Map.of(ConfluentFormatConfig.NUMERIC_MAPPING, "guess"),
            Map.of(ConfluentFormatConfig.NUMERIC_DEFAULT_SCALE, "128"),
            Map.of(ConfluentFormatConfig.DATE_MAPPING, "local"),
            Map.of(ConfluentFormatConfig.KEY_TEMPLATE, "${unknown}"))) {
      assertThatThrownBy(() -> config(bad)).isInstanceOf(ConfigException.class);
    }
  }
}
