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
package sh.oso.connect.oracle.bench.soak;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * The database's current SCN as the workload user, with the first method that user may run:
 *
 * <ol>
 *   <li>{@code v$database}: exact; needs SELECT on {@code V_$DATABASE} (the lab image grants it to
 *       {@code WORKLOAD}).
 *   <li>{@code dbms_flashback}: exact; needs EXECUTE on {@code DBMS_FLASHBACK}.
 *   <li>{@code ledger-ora-rowscn}: no grant beyond owning the ledger. {@code ORA_ROWSCN} is a
 *       conservative upper bound of each row's commit SCN, and every committed workload transaction
 *       writes one ledger row, so the maximum is at or after the last workload commit; {@code
 *       TIMESTAMP_TO_SCN(SYSTIMESTAMP)} (approximate, within about three seconds, no grant) covers
 *       the empty ledger right after a reset, which is why the soak waits before that read.
 * </ol>
 *
 * A method refused with ORA-00942, ORA-01031 or ORA-00904 is skipped for good; any other error is
 * the database's and propagates.
 */
public final class JdbcScnSource implements Soak.ScnSource {

  /** Runs a one-value query on a fresh connection. */
  public interface Query {
    long scalar(String sql) throws SQLException;
  }

  /** A way to read the SCN. */
  public record Method(String name, String sql) {}

  /** Not visible or not permitted to this user: try the next method. */
  static final Set<Integer> REFUSED = Set.of(942, 1031, 904);

  private final Query query;
  private final List<Method> methods;
  private int chosen = -1;
  private String method = "not read";

  public JdbcScnSource(Query query, String ledgerTable) {
    this.query = query;
    this.methods =
        List.of(
            new Method("v$database", "SELECT current_scn FROM v$database"),
            new Method(
                "dbms_flashback", "SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER FROM dual"),
            new Method(
                "ledger-ora-rowscn",
                "SELECT GREATEST(TIMESTAMP_TO_SCN(SYSTIMESTAMP), NVL(MAX(ORA_ROWSCN), 0)) FROM "
                    + ledgerTable));
  }

  @Override
  public synchronized long currentScn() throws SQLException {
    if (chosen >= 0) {
      return query.scalar(methods.get(chosen).sql());
    }
    SQLException last = null;
    for (int i = 0; i < methods.size(); i++) {
      try {
        long scn = query.scalar(methods.get(i).sql());
        chosen = i;
        method = methods.get(i).name();
        return scn;
      } catch (SQLException e) {
        if (!REFUSED.contains(e.getErrorCode())) {
          throw e;
        }
        last = e;
      }
    }
    throw new SQLException(
        "no way to read the current SCN is permitted to this user; grant SELECT on V_$DATABASE"
            + " or EXECUTE on DBMS_FLASHBACK, or keep the ledger table readable",
        last);
  }

  @Override
  public synchronized String method() {
    return method;
  }
}
