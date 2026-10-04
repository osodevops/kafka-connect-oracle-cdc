package sh.oso.connect.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class VersionTest {
  @Test
  void versionIsNeverNull() {
    // Outside a packaged jar the manifest is absent and the fallback applies.
    assertThat(Version.VERSION).isNotNull().isNotBlank();
    assertThat(new OracleCdcSourceConnector().version()).isEqualTo(Version.VERSION);
  }
}
