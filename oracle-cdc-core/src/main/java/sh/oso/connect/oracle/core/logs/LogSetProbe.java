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
package sh.oso.connect.oracle.core.logs;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.errors.OracleCdcException;
import sh.oso.connect.oracle.core.errors.OracleCdcPurgedException;

/**
 * Proves every log in a set is readable by adding it to a throwaway LogMiner session. The catalog
 * does not notice a file removed from disk ({@code reference/mining-errors.md}); ADD_LOGFILE does,
 * with ORA-01284, which becomes {@link OracleCdcPurgedException} (PRD-00 CORE-LOG-4).
 */
public final class LogSetProbe {

  private final OraErrorClassifier classifier;

  public LogSetProbe(OraErrorClassifier classifier) {
    this.classifier = classifier;
  }

  public void probeReadable(Connection mining, LogSet set) throws SQLException {
    try (Statement st = mining.createStatement()) {
      boolean first = true;
      for (RedoLog log : set.logs()) {
        try {
          st.execute(
              "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => '"
                  + log.path().replace("'", "''")
                  + "', OPTIONS => "
                  + (first ? "DBMS_LOGMNR.NEW" : "DBMS_LOGMNR.ADDFILE")
                  + "); END;");
          first = false;
        } catch (SQLException e) {
          OracleCdcException typed = classifier.toException(e, "Adding " + log.describe());
          if (typed instanceof OracleCdcPurgedException) {
            throw new OracleCdcPurgedException(
                "Redo log "
                    + log.describe()
                    + " cannot be read (ORA-"
                    + String.format("%05d", OraErrorClassifier.oraCode(e))
                    + ") although the catalog lists it.",
                "The file was removed outside RMAN. Run RMAN CROSSCHECK ARCHIVELOG ALL, restore it,"
                    + " or run oracle-cdc-admin resnapshot for the captured tables. The connector"
                    + " never skips to a later SCN.",
                e);
          }
          if (typed != null) {
            throw typed;
          }
          throw e;
        }
      }
    } finally {
      try (Statement st = mining.createStatement()) {
        st.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;");
      } catch (SQLException ignore) {
        // no session was started
      }
    }
  }
}
