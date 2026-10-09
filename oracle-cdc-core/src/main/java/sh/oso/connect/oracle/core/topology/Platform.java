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
package sh.oso.connect.oracle.core.topology;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Where the database runs (ADR-0024). Detected, never configured: on Amazon RDS the engine mines
 * exactly as on premises, and only the setup script and the doctor's fix text differ, so a setting
 * could only mislabel a database.
 */
public enum Platform {
  ONPREM,
  RDS,
  AUTONOMOUS;

  /**
   * RDS when the RDSADMIN schema exists (ALL_USERS lists every user, no privilege needed);
   * Autonomous Database when the session reports a cloud service; otherwise on premises.
   */
  public static Platform detect(Connection c) throws SQLException {
    try (Statement s = c.createStatement();
        ResultSet rs =
            s.executeQuery("SELECT COUNT(*) FROM all_users WHERE username = 'RDSADMIN'")) {
      if (rs.next() && rs.getInt(1) > 0) {
        return RDS;
      }
    }
    try (Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT SYS_CONTEXT('USERENV', 'CLOUD_SERVICE') FROM dual")) {
      if (rs.next() && rs.getString(1) != null) {
        return AUTONOMOUS;
      }
    } catch (SQLException e) {
      // ORA-02003: no such context parameter on this release, so not Autonomous Database
    }
    return ONPREM;
  }
}
