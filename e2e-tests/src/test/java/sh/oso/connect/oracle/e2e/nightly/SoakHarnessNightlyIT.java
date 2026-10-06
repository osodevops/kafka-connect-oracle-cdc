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
package sh.oso.connect.oracle.e2e.nightly;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sh.oso.connect.oracle.bench.BenchMain;
import sh.oso.connect.oracle.bench.workload.WorkloadSpec;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * The T3 soak harness end to end (testing strategy section 1): {@code bench soak} runs its paced
 * workload against a real connector for a few minutes, pauses at each check point until the
 * committed position passes the database's SCN, runs the correctness oracle, and writes its
 * evidence and summary. The full soak runs the same command for 72 hours on the compose lab.
 *
 * <p>{@code -Dnightly.soak.hours} sets the length (default 0.15, nine minutes, a check every
 * three).
 */
@Tag("nightly")
class SoakHarnessNightlyIT {

  static final String NAME = "soak-trial";
  static final String PREFIX = "soak";

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void aShortSoakChecksEachSegmentAndWritesItsSummary(@TempDir Path out) throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    double hours = Double.parseDouble(System.getProperty("nightly.soak.hours", "0.15"));
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    WorkloadSpec spec = WorkloadSpec.defaults();
    spec.tables = 3;
    try (ConnectCluster cluster = new ConnectCluster().start()) {
      cluster.register(
          NAME,
          NightlyRun.connector(
              PREFIX, schema, spec, ConnectCluster.oracleDatabaseProps("FREEPDB1")));
      cluster.awaitRunning(NAME, Duration.ofMinutes(3));
      cluster.awaitOffsets(NAME, Duration.ofSeconds(90));

      int exit =
          BenchMain.run(
              "soak",
              "--url",
              db.jdbcUrl(OracleTestDatabase.PDB1),
              "--user",
              schema,
              "--password",
              schema,
              "--hours",
              Double.toString(hours),
              "--check-every",
              Double.toString(hours / 3),
              "--bootstrap-servers",
              cluster.bootstrapServers(),
              "--topic-prefix",
              PREFIX,
              "--pdb",
              "FREEPDB1",
              "--tables",
              String.join(",", NightlyRun.tables(spec)),
              "--connect-url",
              cluster.restUrl(),
              "--connector",
              NAME,
              "--out",
              out.toString());

      Path summary;
      try (Stream<Path> files = Files.walk(out)) {
        summary =
            files
                .filter(p -> p.getFileName().toString().equals("summary.json"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no summary.json under " + out));
      }
      String text = Files.readString(summary);
      System.out.println("soak trial: exit " + exit + "\n" + text);
      assertThat(exit).as(text).isZero();
      JsonNode s = ConnectCluster.json(text);
      JsonNode checks = s.path("checks");
      assertThat(checks.size()).as("periodic checks and the final one").isGreaterThanOrEqualTo(3);
      for (JsonNode c : checks) {
        assertThat(c.path("verdict").asText()).as(c.toString()).isEqualTo("PASS");
      }
      assertThat(text).doesNotContain("password=" + schema);
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }
}
