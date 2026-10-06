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
package sh.oso.connect.oracle.core.errors;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class OraErrorClassifierTest {

  private final OraErrorClassifier classifier = new OraErrorClassifier();

  @ParameterizedTest
  @CsvSource({
    "3113, TRANSIENT_DATABASE",
    "12541, TRANSIENT_DATABASE",
    "1033, TRANSIENT_DATABASE",
    "16331, TRANSIENT_DATABASE",
    "310, MINING_STEP_RETRY",
    "334, MINING_STEP_RETRY",
    "1289, MINING_STEP_RETRY",
    "1291, MINING_STEP_RETRY",
    "1368, MINING_STEP_RETRY",
    "1013, MINING_STEP_RETRY",
    "1284, LOG_PURGED",
    "308, LOG_PURGED",
    "1031, PRIVILEGE",
    "942, PRIVILEGE",
    "1017, PRIVILEGE",
  })
  void codesFromTheReferenceDocumentsAreClassified(int ora, ErrorCode expected) {
    SQLException e = new SQLException("ORA-" + String.format("%05d", ora) + ": test", "99999", ora);
    assertThat(classifier.classify(e)).isEqualTo(expected);
  }

  @Test
  void unknownCodesAreNotClassifiedSoTheyStop() {
    assertThat(classifier.classify(new SQLException("ORA-00001: unique constraint", "23000", 1)))
        .isNull();
    sh.oso.connect.oracle.core.errors.OracleCdcException typed =
        classifier.toException(new SQLException("ORA-00001: x", "23000", 1), "ctx");
    assertThat(typed).as("never null: a null here became a bare NullPointerException").isNotNull();
    assertThat(typed.code()).isEqualTo(ErrorCode.TRANSIENT_DATABASE);
    assertThat(typed.getMessage()).contains("does not classify").contains("ORA-00001");
    assertThat(
            classifier.classify(
                new java.sql.SQLRecoverableException("ORA-17800: minus one", "08000", 17800)))
        .isEqualTo(ErrorCode.TRANSIENT_DATABASE);
    assertThat(
            classifier.classify(
                new SQLException("x", "99999", 0, new java.sql.SQLRecoverableException("gone"))))
        .isEqualTo(ErrorCode.TRANSIENT_DATABASE);
  }

  @Test
  void extraCodesAndNetworkCausesAreTransient() {
    assertThat(
            new OraErrorClassifier(Set.of(12345))
                .classify(new SQLException("ORA-12345: x", "99999", 12345)))
        .isEqualTo(ErrorCode.TRANSIENT_DATABASE);
    assertThat(
            classifier.classify(
                new SQLRecoverableException("IO Error: Connection reset", "08006", 17002)))
        .isEqualTo(ErrorCode.TRANSIENT_DATABASE);
    SQLException wrapped =
        new SQLException(
            "ORA-00604: error occurred at recursive SQL level 1",
            "99999",
            604,
            new IOException("reset"));
    assertThat(classifier.classify(wrapped)).isEqualTo(ErrorCode.TRANSIENT_DATABASE);
    assertThat(classifier.classify(new SQLException("ORA-00604: plain", "99999", 604))).isNull();
  }

  @Test
  void oraCodeIsReadFromCodeOrMessage() {
    assertThat(OraErrorClassifier.oraCode(new SQLException("x", "y", 1284))).isEqualTo(1284);
    assertThat(
            OraErrorClassifier.oraCode(
                new RuntimeException(
                    "wrapped", new SQLException("ORA-01291: missing log file", "99999", 0))))
        .isEqualTo(1291);
    assertThat(OraErrorClassifier.oraCode(new RuntimeException("nothing"))).isEqualTo(-1);
  }

  @Test
  void typedExceptionsCarryCodeActionAndRunbook() {
    OracleCdcException e =
        classifier.toException(
            new SQLException("ORA-01284: file cannot be opened", "99999", 1284),
            "Adding archived log 42");
    assertThat(e).isInstanceOf(OracleCdcPurgedException.class);
    assertThat(e.code()).isEqualTo(ErrorCode.LOG_PURGED);
    assertThat(e.getMessage())
        .contains("Adding archived log 42 failed with ORA-01284")
        .contains("resnapshot")
        .contains("/runbooks/log-purged");
    assertThat(e.retriable()).isFalse();
    assertThat(classifier.toException(new SQLException("ORA-03113", "08006", 3113), "c"))
        .isInstanceOf(TransientDatabaseException.class);
    assertThat(classifier.toException(new SQLException("ORA-00310", "99999", 310), "c"))
        .isInstanceOf(MiningStepRetryException.class);
    assertThat(classifier.toException(new SQLException("ORA-01031", "42000", 1031), "c"))
        .isInstanceOf(PrivilegeException.class);
  }

  @Test
  void everyTypedExceptionMapsToItsCode() {
    assertThat(new OracleCdcGapException("m", "a").code()).isEqualTo(ErrorCode.LOG_GAP);
    assertThat(new DecodeException("m", "a").code()).isEqualTo(ErrorCode.DECODE);
    assertThat(new OracleCdcCorruptionException("m", "a").code()).isEqualTo(ErrorCode.CORRUPTION);
    assertThat(new BufferExhaustedException("m", "a").code()).isEqualTo(ErrorCode.BUFFER_EXHAUSTED);
    assertThat(new JournalCorruptionException("m", "a").code())
        .isEqualTo(ErrorCode.JOURNAL_CORRUPTION);
    assertThat(new MiningStalledException("m", "a").code()).isEqualTo(ErrorCode.MINING_STALLED);
    assertThat(new TopologyException("m", "a").code()).isEqualTo(ErrorCode.TOPOLOGY);
    assertThat(new DictionaryUnavailableException("m", "a").code())
        .isEqualTo(ErrorCode.DICTIONARY_UNAVAILABLE);
    assertThat(new UnsupportedDdlException("m", "a").code()).isEqualTo(ErrorCode.UNSUPPORTED_DDL);
    assertThat(new OrphanReleaseViolationException("m", "a", new RuntimeException()).code())
        .isEqualTo(ErrorCode.ORPHAN_RELEASE_VIOLATION);
  }
}
