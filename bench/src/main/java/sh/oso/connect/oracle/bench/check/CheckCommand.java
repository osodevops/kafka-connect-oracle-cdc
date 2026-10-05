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
package sh.oso.connect.oracle.bench.check;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * {@code bench check}: the correctness oracle as a command. Exit 0 pass, 1 fail, 3 inconclusive.
 */
@Command(
    name = "check",
    mixinStandardHelpOptions = true,
    description = "Compare the connector's topics with the database and the workload ledger.")
public final class CheckCommand implements Callable<Integer> {

  @Option(names = "--bootstrap-servers", required = true, description = "Kafka bootstrap servers.")
  String bootstrap;

  @Option(names = "--url", required = true, description = "JDBC URL of the PDB holding the tables.")
  String url;

  @Option(names = "--user", required = true, description = "Schema owner of the tables.")
  String user;

  @Option(names = "--password", required = true, description = "Password.")
  String password;

  @Option(names = "--topic-prefix", required = true, description = "Connector topic prefix.")
  String prefix;

  @Option(names = "--pdb", description = "PDB name used in topic names (omit for a non-CDB).")
  String pdb;

  @Option(
      names = "--tables",
      split = ",",
      required = true,
      description = "Tables to compare, comma-separated.")
  List<String> tables;

  @Option(
      names = "--ledger-table",
      description = "Workload ledger table (default WL_LEDGER; 'none' skips).")
  String ledger = "WL_LEDGER";

  @Option(names = "--timeout", description = "Seconds to wait for records in total (default 300).")
  int timeoutSeconds = 300;

  @Option(
      names = "--idle",
      description = "Seconds without new records that end consumption (default 20).")
  int idleSeconds = 20;

  @Option(names = "--out", description = "Evidence JSON file.")
  Path out;

  @Override
  public Integer call() throws Exception {
    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    List<String> topics = new ArrayList<>();
    String owner = user.toUpperCase(Locale.ROOT);
    List<String> upper = tables.stream().map(t -> t.trim().toUpperCase(Locale.ROOT)).toList();
    for (String t : upper) {
      topics.add(
          prefix + "." + (pdb == null ? "" : pdb.toUpperCase(Locale.ROOT) + ".") + owner + "." + t);
    }
    try (Connection c = DriverManager.getConnection(url, props)) {
      CorrectnessCheck check =
          new CorrectnessCheck(
              bootstrap,
              topics,
              c,
              owner,
              upper,
              "none".equalsIgnoreCase(ledger) ? null : ledger.toUpperCase(Locale.ROOT),
              Duration.ofSeconds(timeoutSeconds),
              Duration.ofSeconds(idleSeconds));
      CheckReport report = check.run();
      String json = report.toJson();
      if (out != null) {
        report.write(out);
      }
      System.out.println(json);
      return report.exitCode();
    }
  }
}
