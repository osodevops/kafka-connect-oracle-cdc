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
package sh.oso.connect.oracle.e2e.support;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Properties;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * The one Oracle Database Free container shared by every test in a JVM. Built from {@code
 * docker/test-oracle} (ARCHIVELOG, supplemental logging, FREEPDB1 and FREEPDB2, the C##CDC capture
 * user) and tagged by the SHA-256 of that directory, so the image is rebuilt only when its files
 * change. Tests isolate by schema (see {@link SchemaFixtures}); nothing ever restarts the database.
 *
 * <p>Select the base image with {@code -Doracle.image.tag} (default {@code
 * 23.26.3-slim-faststart}). Container reuse across JVMs is honoured only when {@code
 * ~/.testcontainers.properties} enables it.
 */
public final class OracleTestDatabase {

  private static final Logger LOG = LoggerFactory.getLogger(OracleTestDatabase.class);

  public static final String SYS_PASSWORD = "oracle";
  public static final String CAPTURE_USER = "c##cdc";
  public static final String CAPTURE_PASSWORD = "cdc";
  public static final String CDB_SERVICE = "FREE";
  public static final String PDB1 = "FREEPDB1";
  public static final String PDB2 = "FREEPDB2";

  private static volatile OracleTestDatabase instance;
  private static volatile RuntimeException startFailure;

  private final GenericContainer<?> container;
  private final String baseTag;

  private OracleTestDatabase(GenericContainer<?> container, String baseTag) {
    this.container = container;
    this.baseTag = baseTag;
  }

  /** Starts the shared database on first use and returns it. */
  public static OracleTestDatabase get() {
    OracleTestDatabase local = instance;
    if (local == null) {
      synchronized (OracleTestDatabase.class) {
        local = instance;
        if (local == null) {
          if (startFailure != null) {
            throw new IllegalStateException(
                "Oracle test database failed to start earlier in this JVM", startFailure);
          }
          try {
            local = start();
          } catch (RuntimeException e) {
            startFailure = e;
            throw e;
          }
          instance = local;
        }
      }
    }
    return local;
  }

  /** One Docker network for the JVM so Kafka and Connect containers can reach "oracle:1521". */
  public static final org.testcontainers.containers.Network NETWORK =
      org.testcontainers.containers.Network.builder()
          // a fixed subnet: Docker Desktop on a busy workstation exhausts its default address
          // pools ("all predefined address pools have been fully subnetted")
          .createNetworkCmdModifier(
              cmd ->
                  cmd.withIpam(
                      new com.github.dockerjava.api.model.Network.Ipam()
                          .withConfig(
                              new com.github.dockerjava.api.model.Network.Ipam.Config()
                                  .withSubnet(
                                      System.getProperty("e2e.network.subnet", "10.214.0.0/24")))))
          .build();

  public static final String NETWORK_ALIAS = "oracle";

  private static OracleTestDatabase start() {
    String baseTag = System.getProperty("oracle.image.tag", "23.26.3-slim-faststart");
    Path dockerDir =
        Path.of(System.getProperty("repo.root", ".."))
            .resolve("docker/test-oracle")
            .toAbsolutePath()
            .normalize();
    if (!Files.isDirectory(dockerDir)) {
      throw new IllegalStateException("docker/test-oracle not found at " + dockerDir);
    }
    String imageName = "oracle-cdc-test-db:" + baseTag + "-" + directoryDigest(dockerDir);
    LOG.info("Oracle test database image {} from {}", imageName, dockerDir);

    ImageFromDockerfile image =
        new ImageFromDockerfile(imageName, false)
            .withFileFromPath(".", dockerDir)
            .withBuildArg("BASE_TAG", baseTag);

    GenericContainer<?> container =
        new GenericContainer<>(image)
            .withExposedPorts(1521)
            .withNetwork(NETWORK)
            .withNetworkAliases(NETWORK_ALIAS)
            .withEnv("ORACLE_PASSWORD", SYS_PASSWORD)
            .withSharedMemorySize(2L * 1024 * 1024 * 1024)
            .withReuse(true)
            .withLogConsumer(
                frame -> {
                  String line = frame.getUtf8StringWithoutLineEnding();
                  if (line.contains("oracle-cdc-test-db:")
                      || line.contains("CONTAINER:")
                      || line.contains("ORA-")
                      || line.contains("ASSERT")
                      || line.contains("READY TO USE")) {
                    LOG.info("[oracle] {}", line);
                  }
                })
            .waitingFor(
                new ArchivelogReadyWaitStrategy().withStartupTimeout(Duration.ofMinutes(6)));
    long t0 = System.nanoTime();
    container.start();
    LOG.info(
        "Oracle test database ready in {} s on port {}",
        Duration.ofNanos(System.nanoTime() - t0).toSeconds(),
        container.getMappedPort(1521));
    return new OracleTestDatabase(container, baseTag);
  }

  public GenericContainer<?> container() {
    return container;
  }

  public String baseTag() {
    return baseTag;
  }

  /** JDBC URL for a service: {@code FREE} for CDB$ROOT, or a PDB name. */
  public String jdbcUrl(String service) {
    return "jdbc:oracle:thin:@//"
        + container.getHost()
        + ":"
        + container.getMappedPort(1521)
        + "/"
        + service;
  }

  /** Connection as the capture user. {@code FREE} connects to CDB$ROOT, where mining happens. */
  public Connection capture(String service) throws SQLException {
    return DriverManager.getConnection(jdbcUrl(service), CAPTURE_USER, CAPTURE_PASSWORD);
  }

  /** Connection as an arbitrary user, for workload sessions inside a PDB. */
  public Connection connect(String service, String user, String password) throws SQLException {
    return DriverManager.getConnection(jdbcUrl(service), user, password);
  }

  /** SYSDBA connection for test setup, teardown and fault injection. Never used by the engine. */
  public Connection sysdba(String service) throws SQLException {
    Properties p = new Properties();
    p.setProperty("user", "sys");
    p.setProperty("password", SYS_PASSWORD);
    p.setProperty("internal_logon", "sysdba");
    return DriverManager.getConnection(jdbcUrl(service), p);
  }

  /** SYSDBA connection to a PDB through the root service, switching container after connect. */
  public Connection sysdbaInPdb(String pdb) throws SQLException {
    Connection c = sysdba(CDB_SERVICE);
    try (var st = c.createStatement()) {
      st.execute("ALTER SESSION SET CONTAINER = " + pdb);
    }
    return c;
  }

  private static String directoryDigest(Path dir) {
    try (Stream<Path> files = Files.walk(dir)) {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      files
          .filter(Files::isRegularFile)
          .sorted()
          .forEach(
              f -> {
                try {
                  md.update(dir.relativize(f).toString().getBytes());
                  md.update(Files.readAllBytes(f));
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
      return HexFormat.of().formatHex(md.digest()).substring(0, 12);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
