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
package sh.oso.connect.oracle.core.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class CoreConfigTest {

  static Map<String, String> minimal() {
    Map<String, String> p = new HashMap<>();
    p.put(CoreConfig.DATABASE_HOST, "oracle");
    p.put(CoreConfig.DATABASE_SERVICE, "FREE");
    p.put(CoreConfig.DATABASE_USER, "C##CDC");
    p.put(CoreConfig.DATABASE_PASSWORD, "cdc");
    return p;
  }

  @Test
  void everyKeyIsPrefixedGroupedAndDocumented() {
    for (ConfigDef.ConfigKey k : CoreConfig.configDef().configKeys().values()) {
      assertThat(k.name).startsWith("cdc.");
      assertThat(k.group).as(k.name).isNotBlank();
      assertThat(k.displayName).as(k.name).isNotBlank();
      assertThat(k.documentation).as(k.name).isNotBlank();
    }
    assertThat(CoreConfig.configDef().names()).hasSize(58);
  }

  @Test
  void defaultsMatchTheProductRequirements() {
    CoreConfig c = new CoreConfig(minimal());
    assertThat(c.getInt(CoreConfig.DATABASE_PORT)).isEqualTo(1521);
    assertThat(c.captureMode()).isEqualTo(CoreConfig.CaptureMode.ONLINE);
    assertThat(c.getLong(CoreConfig.MINING_TARGET_LATENCY_MS)).isEqualTo(2000L);
    assertThat(c.getInt(CoreConfig.MINING_MAX_LOGS_PER_STEP)).isEqualTo(8);
    assertThat(c.getLong(CoreConfig.BUFFER_MEMORY_MAX_BYTES)).isEqualTo(268435456L);
    assertThat(c.getLong(CoreConfig.BUFFER_SPILL_MAX_BYTES)).isEqualTo(10737418240L);
    assertThat(c.getLong(CoreConfig.TXJOURNAL_THRESHOLD_MS)).isEqualTo(300000L);
    assertThat(c.getLong(CoreConfig.TRANSACTION_MAX_AGE_MS)).isEqualTo(-1L);
    assertThat(c.maxAgeAction()).isEqualTo(CoreConfig.MaxAgeAction.FAIL);
    // SRC-LOB-1, SRC-LOB-2
    assertThat(c.lobMode()).isEqualTo(CoreConfig.LobMode.SKIP);
    assertThat(c.getLong(CoreConfig.LOB_MAX_BYTES)).isEqualTo(1048576L);
    assertThat(c.lobOversizeAction()).isEqualTo(CoreConfig.LobOversizeAction.FAIL);
    assertThat(c.getString(CoreConfig.UNAVAILABLE_PLACEHOLDER))
        .isEqualTo("__cdc_unavailable_value");
    assertThat(c.orphanAction()).isEqualTo(CoreConfig.OrphanAction.RELEASE);
    assertThat(c.decodeErrorAction()).isEqualTo(CoreConfig.DecodeErrorAction.FAIL);
    assertThat(c.getLong(CoreConfig.RETRY_MAX_TIME_MS)).isEqualTo(86400000L);
    assertThat(c.getBoolean(CoreConfig.LOG_SENSITIVE_DATA)).isFalse();
    assertThat(c.pdbs()).isEmpty();
    assertThat(c.extraRetryErrorCodes()).isEmpty();
  }

  @Test
  void enumsAcceptAnyCaseAndRejectUnknownValues() {
    Map<String, String> p = minimal();
    p.put(CoreConfig.CAPTURE_MODE, "ARCHIVE_ONLY");
    p.put(CoreConfig.ON_DECODE_ERROR, "Dlq");
    CoreConfig c = new CoreConfig(p);
    assertThat(c.captureMode()).isEqualTo(CoreConfig.CaptureMode.ARCHIVE_ONLY);
    assertThat(c.decodeErrorAction()).isEqualTo(CoreConfig.DecodeErrorAction.DLQ);
    Map<String, String> bad = minimal();
    bad.put(CoreConfig.TRANSACTION_ORPHAN_ACTION, "ignore");
    assertThatThrownBy(() -> new CoreConfig(bad)).isInstanceOf(ConfigException.class);
  }

  @Test
  void extraRetryCodesAcceptOraPrefix() {
    Map<String, String> p = minimal();
    p.put(CoreConfig.RETRY_EXTRA_ERROR_CODES, "ORA-12345, 54321");
    assertThat(new CoreConfig(p).extraRetryErrorCodes()).containsExactly(12345, 54321);
  }

  @Test
  void connectionAndRangeValidation() {
    Map<String, String> noHost = minimal();
    noHost.remove(CoreConfig.DATABASE_HOST);
    assertThatThrownBy(() -> new CoreConfig(noHost))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("cdc.database.url");
    Map<String, String> noService = minimal();
    noService.remove(CoreConfig.DATABASE_SERVICE);
    assertThatThrownBy(() -> new CoreConfig(noService))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("cdc.database.sid");
    Map<String, String> url = minimal();
    url.remove(CoreConfig.DATABASE_HOST);
    url.remove(CoreConfig.DATABASE_SERVICE);
    url.put(CoreConfig.DATABASE_URL, "jdbc:oracle:thin:@//h:1521/S");
    assertThat(new CoreConfig(url).getString(CoreConfig.DATABASE_URL)).isNotBlank();
    Map<String, String> age = minimal();
    age.put(CoreConfig.TRANSACTION_MAX_AGE_MS, "10");
    assertThatThrownBy(() -> new CoreConfig(age)).isInstanceOf(ConfigException.class);
    Map<String, String> small = minimal();
    small.put(CoreConfig.BUFFER_MEMORY_MAX_BYTES, "1");
    assertThatThrownBy(() -> new CoreConfig(small)).isInstanceOf(ConfigException.class);
  }

  @Test
  void passwordIsHidden() {
    CoreConfig c = new CoreConfig(minimal());
    assertThat(c.databasePassword().value()).isEqualTo("cdc");
    assertThat(c.toString()).doesNotContain("cdc\"").doesNotContain("= cdc");
  }
}
