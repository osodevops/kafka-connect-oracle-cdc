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
package sh.oso.connect.oracle.validation;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.core.config.CoreConfig;

class ConnectorValidatorTest {

  /** Each doctor finding lands on the setting an operator would change for it. */
  @Test
  void findingsLandOnTheSettingAnOperatorWouldChange() {
    assertThat(ConnectorValidator.keyFor("DOC-3"))
        .isEqualTo(OracleCdcSourceConnectorConfig.TABLES_INCLUDE);
    assertThat(ConnectorValidator.keyFor("DOC-4")).isEqualTo(CoreConfig.DATABASE_USER);
    assertThat(ConnectorValidator.keyFor("DOC-12")).isEqualTo(CoreConfig.ARCHIVE_DESTINATION);
    // ADR-0023: a RAC database is refused; the database, not a setting, is what has to change
    assertThat(ConnectorValidator.keyFor("DOC-13")).isEqualTo(CoreConfig.DATABASE_HOST);
    assertThat(ConnectorValidator.keyFor("DOC-14")).isEqualTo(CoreConfig.CAPTURE_MODE);
    assertThat(ConnectorValidator.keyFor("DOC-99")).isEqualTo(CoreConfig.DATABASE_HOST);
  }
}
