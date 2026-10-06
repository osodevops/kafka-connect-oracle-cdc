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
package sh.oso.connect.oracle.bench.soak;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

/** Reading the committed position and a failed task from the Connect REST answers. */
class ConnectRestProbeTest {

  static JsonNode json(String s) throws Exception {
    return new ObjectMapper().readTree(s);
  }

  @Test
  void theLowestResumeScnOverTheOffsetsIsThePosition() throws Exception {
    assertThat(
            ConnectRestProbe.minResumeScn(
                json(
                    "{\"offsets\":[{\"partition\":{\"server\":\"cdc\"},"
                        + "\"offset\":{\"v\":1,\"resume_scn\":5000,\"last_commit_scn\":4990}}]}")))
        .hasValue(5000);
    // several partitions, a string value, one entry without a resume SCN
    assertThat(
            ConnectRestProbe.minResumeScn(
                json(
                    "{\"offsets\":["
                        + "{\"partition\":{\"server\":\"a\"},\"offset\":{\"resume_scn\":\"7000\"}},"
                        + "{\"partition\":{\"server\":\"b\"},\"offset\":{\"resume_scn\":6500}},"
                        + "{\"partition\":{\"x\":1},\"offset\":{\"other\":1}}]}")))
        .hasValue(6500);
  }

  @Test
  void noOffsetYetIsEmpty() throws Exception {
    assertThat(ConnectRestProbe.minResumeScn(json("{\"offsets\":[]}"))).isEmpty();
    assertThat(
            ConnectRestProbe.minResumeScn(
                json("{\"offsets\":[{\"partition\":{},\"offset\":{\"resume_scn\":\"n/a\"}}]}")))
        .isEmpty();
  }

  @Test
  void aFailedTaskIsNamedWithItsCodeButNotItsTrace() throws Exception {
    assertThat(
            ConnectRestProbe.failureOf(
                json(
                    "{\"connector\":{\"state\":\"RUNNING\"},"
                        + "\"tasks\":[{\"id\":0,\"state\":\"RUNNING\"}]}")))
        .isNull();
    String failed =
        ConnectRestProbe.failureOf(
            json(
                "{\"connector\":{\"state\":\"RUNNING\"},\"tasks\":[{\"id\":0,\"state\":\"FAILED\","
                    + "\"trace\":\"sh.oso.connect.oracle.core.OracleCdcException: CDC-3001: cannot"
                    + " decode row value 'Alice Example'\\n\\tat x.y\"}]}"));
    assertThat(failed).startsWith("task 0 FAILED with CDC-3001").doesNotContain("Alice");
    assertThat(
            ConnectRestProbe.failureOf(
                json(
                    "{\"connector\":{\"state\":\"FAILED\",\"trace\":\"org.apache.kafka.X\"},"
                        + "\"tasks\":[]}")))
        .isEqualTo("connector FAILED (no CDC code: a framework or Kafka failure)");
  }
}
