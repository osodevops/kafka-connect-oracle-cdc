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
package sh.oso.connect.oracle.doctor.admin;

import java.sql.SQLException;
import java.time.Duration;
import sh.oso.connect.oracle.core.doctor.DoctorCatalog;
import sh.oso.connect.oracle.core.doctor.RedoSampler;
import sh.oso.connect.oracle.core.orphan.TransactionProbe;

/** The database access of the doctor and admin commands; {@link JdbcDatabase} or a test fake. */
public interface Database extends AutoCloseable {

  DoctorCatalog catalog();

  TransactionProbe transactions();

  RedoSampler sampler(Duration timeout);

  /**
   * Reads listed redo logs by adding them to a LogMiner session, so a file removed outside RMAN is
   * found before an offset changes; none by default.
   */
  default sh.oso.connect.oracle.core.doctor.RedoAvailability.LogProbe logProbe() {
    return sh.oso.connect.oracle.core.doctor.RedoAvailability.LogProbe.NONE;
  }

  /** How long ago the SCN was, from SCN_TO_TIMESTAMP, or null when the database no longer knows. */
  Duration scnAge(long scn) throws SQLException;

  @Override
  void close();
}
