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
package sh.oso.connect.oracle.core.orphan;

import java.sql.SQLException;
import java.util.Set;
import sh.oso.connect.oracle.core.model.TxKey;

/** What the orphan detector asks the database (CORE-TX-7, ADR-0006). */
public interface TransactionProbe {

  /** Transactions currently open according to GV$TRANSACTION, keyed by container and XID. */
  Set<TxKey> activeTransactions() throws SQLException;

  /** Whether the session that owned a transaction still exists (SID and SERIAL#). */
  boolean sessionExists(long sessionNo, long serialNo) throws SQLException;

  /** The database's current SCN, taken in the same round trip as the transaction list. */
  long currentScn() throws SQLException;
}
