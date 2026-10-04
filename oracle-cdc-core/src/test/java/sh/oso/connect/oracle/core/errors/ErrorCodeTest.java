package sh.oso.connect.oracle.core.errors;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class ErrorCodeTest {

  private static final Pattern CODE = Pattern.compile("CDC-\\d{4}");
  private static final Pattern SLUG = Pattern.compile("[a-z0-9]+(-[a-z0-9]+)*");

  @Test
  void everyCodeIsUniqueAndWellFormed() {
    Set<String> codes = new HashSet<>();
    Set<String> slugs = new HashSet<>();
    for (ErrorCode c : ErrorCode.values()) {
      assertThat(c.code()).matches(CODE);
      assertThat(c.runbookSlug()).matches(SLUG);
      assertThat(codes.add(c.code())).as("duplicate code %s", c.code()).isTrue();
      assertThat(slugs.add(c.runbookSlug())).as("duplicate slug %s", c.runbookSlug()).isTrue();
      assertThat(c.runbookUrl()).startsWith("https://kafkacdcconnector.com/runbooks/");
    }
  }

  @Test
  void onlyTransientAndStepRetryAreRetriable() {
    for (ErrorCode c : ErrorCode.values()) {
      boolean expected = c == ErrorCode.TRANSIENT_DATABASE || c == ErrorCode.MINING_STEP_RETRY;
      assertThat(c.retriable()).as("%s retriable", c).isEqualTo(expected);
    }
  }

  @Test
  void exceptionMessageCarriesCodeActionAndRunbook() {
    OracleCdcException e =
        new OracleCdcException(
            ErrorCode.LOG_PURGED,
            "Archived log thread 1 sequence 42 (SCN 1000 to 2000) has been deleted.",
            "Run oracle-cdc-admin resnapshot --tables for the affected tables.");
    assertThat(e.getMessage())
        .startsWith("[CDC-2002] Archived log thread 1")
        .contains("Operator action: Run oracle-cdc-admin resnapshot")
        .endsWith("Runbook: https://kafkacdcconnector.com/runbooks/log-purged");
    assertThat(e.retriable()).isFalse();
    assertThat(e.code()).isEqualTo(ErrorCode.LOG_PURGED);
  }
}
