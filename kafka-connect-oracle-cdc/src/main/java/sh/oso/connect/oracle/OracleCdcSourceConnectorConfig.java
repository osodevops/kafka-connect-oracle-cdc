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
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

/**
 * Connector-level configuration ({@code cdc.*} namespace, PRD-01 section 5). The engine-level keys
 * (PRD-00 section 5) are composed in from {@code oracle-cdc-core} as the core module grows; this
 * first version holds only what the bootstrap connector needs.
 */
public class OracleCdcSourceConnectorConfig extends AbstractConfig {

  public static final String GROUP_DATABASE = "Database";
  public static final String GROUP_CAPTURE = "Capture";
  public static final String GROUP_TOPICS = "Topics";

  public static final String DATABASE_HOST = "cdc.database.host";
  public static final String DATABASE_PORT = "cdc.database.port";
  public static final String DATABASE_SERVICE = "cdc.database.service";
  public static final String DATABASE_SID = "cdc.database.sid";
  public static final String DATABASE_URL = "cdc.database.url";
  public static final String DATABASE_USER = "cdc.database.user";
  public static final String DATABASE_PASSWORD = "cdc.database.password";
  public static final String DATABASE_PDBS = "cdc.database.pdbs";

  public static final String TABLES_INCLUDE = "cdc.tables.include";
  public static final String TABLES_EXCLUDE = "cdc.tables.exclude";

  public static final String TOPIC_PREFIX = "cdc.topic.prefix";

  public OracleCdcSourceConnectorConfig(Map<String, String> props) {
    super(configDef(), props);
    validateConnection();
  }

  private void validateConnection() {
    boolean hasUrl = getString(DATABASE_URL) != null && !getString(DATABASE_URL).isBlank();
    boolean hasHost = getString(DATABASE_HOST) != null && !getString(DATABASE_HOST).isBlank();
    boolean hasService =
        getString(DATABASE_SERVICE) != null && !getString(DATABASE_SERVICE).isBlank();
    boolean hasSid = getString(DATABASE_SID) != null && !getString(DATABASE_SID).isBlank();
    if (!hasUrl && !hasHost) {
      throw new ConfigException(
          DATABASE_HOST, null, "Set either " + DATABASE_URL + " or " + DATABASE_HOST + ".");
    }
    if (hasHost && !hasUrl && !hasService && !hasSid) {
      throw new ConfigException(
          DATABASE_SERVICE,
          null,
          "Set "
              + DATABASE_SERVICE
              + " or "
              + DATABASE_SID
              + " together with "
              + DATABASE_HOST
              + ".");
    }
  }

  public String topicPrefix() {
    return getString(TOPIC_PREFIX);
  }

  public List<String> tablesInclude() {
    return getList(TABLES_INCLUDE);
  }

  public List<String> tablesExclude() {
    return getList(TABLES_EXCLUDE);
  }

  public Password databasePassword() {
    return getPassword(DATABASE_PASSWORD);
  }

  public static ConfigDef configDef() {
    ConfigDef def = new ConfigDef();
    int order = 0;
    def.define(
        DATABASE_HOST,
        Type.STRING,
        null,
        Importance.HIGH,
        "Oracle Database host. Alternative to " + DATABASE_URL + ".",
        GROUP_DATABASE,
        ++order,
        Width.MEDIUM,
        "Host");
    def.define(
        DATABASE_PORT,
        Type.INT,
        1521,
        ConfigDef.Range.between(1, 65535),
        Importance.MEDIUM,
        "Oracle listener port.",
        GROUP_DATABASE,
        ++order,
        Width.SHORT,
        "Port");
    def.define(
        DATABASE_SERVICE,
        Type.STRING,
        null,
        Importance.HIGH,
        "Service name (preferred over SID). In a CDB this is the CDB$ROOT service.",
        GROUP_DATABASE,
        ++order,
        Width.MEDIUM,
        "Service");
    def.define(
        DATABASE_SID,
        Type.STRING,
        null,
        Importance.LOW,
        "SID, for databases without a service name.",
        GROUP_DATABASE,
        ++order,
        Width.MEDIUM,
        "SID");
    def.define(
        DATABASE_URL,
        Type.STRING,
        null,
        Importance.MEDIUM,
        "Full JDBC URL (TNS descriptor, LDAP naming or wallet). Overrides host, port, service and"
            + " SID.",
        GROUP_DATABASE,
        ++order,
        Width.LONG,
        "JDBC URL");
    def.define(
        DATABASE_USER,
        Type.STRING,
        ConfigDef.NO_DEFAULT_VALUE,
        Importance.HIGH,
        "Mining user. In a CDB this must be a common user (C## prefix) with the grants from"
            + " oracle-cdc-doctor setup-sql.",
        GROUP_DATABASE,
        ++order,
        Width.MEDIUM,
        "User");
    def.define(
        DATABASE_PASSWORD,
        Type.PASSWORD,
        ConfigDef.NO_DEFAULT_VALUE,
        Importance.HIGH,
        "Password for the mining user. Use a Connect config provider; the value is never logged.",
        GROUP_DATABASE,
        ++order,
        Width.MEDIUM,
        "Password");
    def.define(
        DATABASE_PDBS,
        Type.LIST,
        "",
        Importance.HIGH,
        "Pluggable databases to capture, comma separated. Leave empty for a non-CDB database.",
        GROUP_DATABASE,
        ++order,
        Width.LONG,
        "PDBs");

    def.define(
        TABLES_INCLUDE,
        Type.LIST,
        ConfigDef.NO_DEFAULT_VALUE,
        Importance.HIGH,
        "Comma-separated regular expressions over PDB.SCHEMA.TABLE (CDB) or SCHEMA.TABLE (non-CDB)"
            + " selecting the tables to capture.",
        GROUP_CAPTURE,
        ++order,
        Width.LONG,
        "Tables to include");
    def.define(
        TABLES_EXCLUDE,
        Type.LIST,
        "",
        Importance.MEDIUM,
        "Comma-separated regular expressions excluding tables that the include patterns matched.",
        GROUP_CAPTURE,
        ++order,
        Width.LONG,
        "Tables to exclude");

    def.define(
        TOPIC_PREFIX,
        Type.STRING,
        ConfigDef.NO_DEFAULT_VALUE,
        new ConfigDef.NonEmptyString(),
        Importance.HIGH,
        "Logical name of this connector and prefix of every topic it writes, including the internal"
            + " schema, journal, ops, heartbeat and signal topics.",
        GROUP_TOPICS,
        ++order,
        Width.MEDIUM,
        "Topic prefix");
    return def;
  }
}
