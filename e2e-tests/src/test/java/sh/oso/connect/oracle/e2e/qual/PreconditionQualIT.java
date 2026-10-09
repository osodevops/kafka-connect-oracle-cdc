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
package sh.oso.connect.oracle.e2e.qual;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.doctor.Doctor;
import sh.oso.connect.oracle.core.doctor.DoctorContext;
import sh.oso.connect.oracle.core.doctor.Finding;
import sh.oso.connect.oracle.core.doctor.JdbcDoctorCatalog;
import sh.oso.connect.oracle.core.doctor.Report;
import sh.oso.connect.oracle.core.doctor.Rules;
import sh.oso.connect.oracle.core.doctor.Severity;
import sh.oso.connect.oracle.e2e.support.Evidence;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;
import sh.oso.connect.oracle.e2e.support.TestDatabase;

/**
 * Qualification (T4): the target is the platform it should be, the capture user has every grant the
 * engine needs (on RDS, the ones {@code setup-sql --platform rds} gives), and the doctor's fast
 * rules, which {@code validate()} runs, find nothing blocking on a clean captured table.
 */
@Tag("qual")
class PreconditionQualIT {

  private final TestDatabase db = TestDatabase.get();

  @Test
  void theDoctorFindsNothingBlockingAndThePlatformIsDetected() throws Exception {
    Evidence ev = Evidence.of(getClass(), "precondition").param("target", db.describe());
    String schema = SchemaFixtures.nameFor(getClass());
    try {
      db.recreateSchema(schema);
      try (Connection w = db.workload(schema);
          Statement s = w.createStatement()) {
        s.execute("CREATE TABLE clean (id NUMBER PRIMARY KEY, name VARCHAR2(50))");
        s.execute("ALTER TABLE clean ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      try (Connection c = db.capture()) {
        JdbcDoctorCatalog catalog = new JdbcDoctorCatalog(c);
        assertThat(catalog.platform()).isEqualTo(db.platform());
        Report r =
            new Doctor(Rules.fastMode())
                .run(
                    new DoctorContext(
                        db.coreConfig(),
                        catalog,
                        List.of(db.include(schema, "CLEAN")),
                        List.of(),
                        "fail"));
        r.findings().forEach(f -> ev.note(f.rule() + " " + f.severity() + ": " + f.message()));
        ev.count("findings", r.findings().size());
        assertThat(r.findings())
            .filteredOn(f -> f.severity() == Severity.BLOCKING)
            .extracting(Finding::rule, Finding::message)
            .isEmpty();
        assertThat(catalog.canExecute("SYS", "DBMS_LOGMNR_D"))
            .as("dictionary builds need EXECUTE ON DBMS_LOGMNR_D")
            .isTrue();
      }
    } catch (Throwable t) {
      ev.failed(t);
      throw t;
    } finally {
      ev.write();
      db.dropSchema(schema);
    }
  }
}
