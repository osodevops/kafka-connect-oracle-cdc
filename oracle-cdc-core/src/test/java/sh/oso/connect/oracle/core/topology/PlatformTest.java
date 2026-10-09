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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

class PlatformTest {

  private static Connection database(int rdsadminUsers, String cloudService, boolean noContext)
      throws SQLException {
    Connection c = mock(Connection.class);
    Statement s = mock(Statement.class);
    when(c.createStatement()).thenReturn(s);
    ResultSet users = mock(ResultSet.class);
    when(users.next()).thenReturn(true);
    when(users.getInt(1)).thenReturn(rdsadminUsers);
    when(s.executeQuery(contains("RDSADMIN"))).thenReturn(users);
    if (noContext) {
      when(s.executeQuery(contains("CLOUD_SERVICE")))
          .thenThrow(new SQLException("ORA-02003: invalid USERENV parameter", "HY000", 2003));
    } else {
      ResultSet cloud = mock(ResultSet.class);
      when(cloud.next()).thenReturn(true);
      when(cloud.getString(1)).thenReturn(cloudService);
      when(s.executeQuery(contains("CLOUD_SERVICE"))).thenReturn(cloud);
    }
    return c;
  }

  @Test
  void theRdsadminSchemaMarksAmazonRds() throws SQLException {
    assertThat(Platform.detect(database(1, null, false))).isEqualTo(Platform.RDS);
  }

  @Test
  void aCloudServiceMarksAutonomousDatabase() throws SQLException {
    assertThat(Platform.detect(database(0, "OLTP", false))).isEqualTo(Platform.AUTONOMOUS);
  }

  @Test
  void anythingElseIsOnPremises() throws SQLException {
    assertThat(Platform.detect(database(0, null, false))).isEqualTo(Platform.ONPREM);
    // releases without the CLOUD_SERVICE parameter are not Autonomous Database
    assertThat(Platform.detect(database(0, null, true))).isEqualTo(Platform.ONPREM);
  }
}
