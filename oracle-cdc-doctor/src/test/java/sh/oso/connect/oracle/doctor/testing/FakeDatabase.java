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
package sh.oso.connect.oracle.doctor.testing;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import sh.oso.connect.oracle.core.doctor.DoctorCatalog;
import sh.oso.connect.oracle.core.doctor.FakeDoctorCatalog;
import sh.oso.connect.oracle.core.doctor.RedoProfile;
import sh.oso.connect.oracle.core.doctor.RedoSampler;
import sh.oso.connect.oracle.core.logs.RedoLog;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.orphan.TransactionProbe;
import sh.oso.connect.oracle.doctor.admin.Database;

/** A database made of the core fakes: catalog, GV$TRANSACTION and a LogMiner sample. */
public final class FakeDatabase implements Database {

  public final FakeDoctorCatalog catalog = new FakeDoctorCatalog();
  public final Set<TxKey> active = new HashSet<>();
  public final Map<Long, Duration> ages = new HashMap<>();
  public List<RedoProfile.SampleRow> sample = List.of();
  public List<RedoLog> sampledLogs;
  public long[] sampledRange;
  public boolean closed;

  @Override
  public DoctorCatalog catalog() {
    return catalog;
  }

  @Override
  public TransactionProbe transactions() {
    return new TransactionProbe() {
      @Override
      public Set<TxKey> activeTransactions() {
        return active;
      }

      @Override
      public boolean sessionExists(long sessionNo, long serialNo) {
        return false;
      }

      @Override
      public long currentScn() {
        return catalog.base.currentScn;
      }
    };
  }

  @Override
  public RedoSampler sampler(Duration timeout) {
    return (logs, start, end) -> {
      sampledLogs = logs;
      sampledRange = new long[] {start, end};
      return sample;
    };
  }

  @Override
  public Duration scnAge(long scn) {
    return ages.get(scn);
  }

  @Override
  public void close() {
    closed = true;
  }
}
