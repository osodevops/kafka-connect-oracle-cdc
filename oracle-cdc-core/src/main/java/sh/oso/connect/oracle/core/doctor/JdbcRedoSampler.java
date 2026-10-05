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
package sh.oso.connect.oracle.core.doctor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import sh.oso.connect.oracle.core.logs.RedoLog;
import sh.oso.connect.oracle.core.mining.DictionaryMode;
import sh.oso.connect.oracle.core.mining.JdbcLogMinerSession;

/**
 * {@link RedoSampler} over a LogMiner session with the online catalog: the logs are added, the
 * range is started, one aggregate query counts the rows, and the session ends. The connection stays
 * open for the caller. Exercised by the engine tier, excluded from the unit coverage gate.
 */
public final class JdbcRedoSampler implements RedoSampler {

  static final String QUERY =
      "SELECT src_con_name, seg_owner, table_name, operation, COUNT(*), SUM(CASE WHEN operation"
          + " = 'DDL' AND UPPER(LTRIM(SUBSTR(sql_redo, 1, 40))) LIKE 'TRUNCATE%' THEN 1 ELSE 0"
          + " END) FROM v$logmnr_contents GROUP BY src_con_name, seg_owner, table_name,"
          + " operation";

  private final Connection c;
  private final Duration timeout;

  public JdbcRedoSampler(Connection miningConnection, Duration timeout) {
    this.c = miningConnection;
    this.timeout = timeout;
  }

  @Override
  public List<RedoProfile.SampleRow> sample(List<RedoLog> logs, long startScn, long endScn)
      throws SQLException {
    JdbcLogMinerSession session = new JdbcLogMinerSession(c, 1000, timeout);
    List<RedoProfile.SampleRow> out = new ArrayList<>();
    try {
      session.setLogs(logs);
      session.start(startScn, endScn, DictionaryMode.ONLINE_CATALOG);
      try (PreparedStatement ps = c.prepareStatement(QUERY)) {
        ps.setQueryTimeout((int) Math.max(0, timeout.toSeconds()));
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            out.add(
                new RedoProfile.SampleRow(
                    rs.getString(1),
                    rs.getString(2),
                    rs.getString(3),
                    rs.getString(4),
                    rs.getLong(5),
                    rs.getLong(6)));
          }
        }
      }
    } finally {
      session.end();
    }
    return out;
  }
}
