package sh.oso.connect.oracle;

/** Connector version, read from the jar manifest (Implementation-Version set by the build). */
public final class Version {
  public static final String VERSION = resolve();

  private static String resolve() {
    String version = Version.class.getPackage().getImplementationVersion();
    return version != null ? version : "unknown";
  }

  private Version() {}
}
