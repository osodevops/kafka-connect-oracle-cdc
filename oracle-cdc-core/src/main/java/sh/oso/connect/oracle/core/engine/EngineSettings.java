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
package sh.oso.connect.oracle.core.engine;

import java.time.Duration;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.config.CoreConfig.DecodeErrorAction;

/**
 * The engine's knobs, lifted out of {@link CoreConfig} so the fake-driven tests need no ConfigDef.
 */
public record EngineSettings(
    Duration targetLatency,
    int maxLogsPerStep,
    Duration sessionMaxAge,
    Duration idlePoll,
    int maxConsecutiveRetries,
    DecodeErrorAction onDecodeError,
    Duration transactionMaxAge,
    MaxAgeAction maxAgeAction,
    LobAssembler.Mode lobMode,
    long lobMaxBytes,
    boolean lobOversizeFail) {

  /** CORE-TX-6: what happens to a transaction open longer than {@code transactionMaxAge}. */
  public enum MaxAgeAction {
    FAIL,
    DISCARD
  }

  /** The pre-CORE-DEC-6 shape: LOB values inline up to 1 MiB, an oversize value stops. */
  public EngineSettings(
      Duration targetLatency,
      int maxLogsPerStep,
      Duration sessionMaxAge,
      Duration idlePoll,
      int maxConsecutiveRetries,
      DecodeErrorAction onDecodeError,
      Duration transactionMaxAge,
      MaxAgeAction maxAgeAction) {
    this(
        targetLatency,
        maxLogsPerStep,
        sessionMaxAge,
        idlePoll,
        maxConsecutiveRetries,
        onDecodeError,
        transactionMaxAge,
        maxAgeAction,
        LobAssembler.Mode.INLINE,
        1L << 20,
        true);
  }

  /** The same knobs with another LOB handling (CORE-DEC-6, SRC-LOB). */
  public EngineSettings withLobs(LobAssembler.Mode mode, long maxBytes, boolean oversizeFail) {
    return new EngineSettings(
        targetLatency,
        maxLogsPerStep,
        sessionMaxAge,
        idlePoll,
        maxConsecutiveRetries,
        onDecodeError,
        transactionMaxAge,
        maxAgeAction,
        mode,
        maxBytes,
        oversizeFail);
  }

  /** The pre-CORE-TX-6 shape: no age limit. */
  public EngineSettings(
      Duration targetLatency,
      int maxLogsPerStep,
      Duration sessionMaxAge,
      Duration idlePoll,
      int maxConsecutiveRetries,
      DecodeErrorAction onDecodeError) {
    this(
        targetLatency,
        maxLogsPerStep,
        sessionMaxAge,
        idlePoll,
        maxConsecutiveRetries,
        onDecodeError,
        null,
        MaxAgeAction.FAIL);
  }

  public static EngineSettings from(CoreConfig c) {
    long maxAge = c.getLong(CoreConfig.TRANSACTION_MAX_AGE_MS);
    return new EngineSettings(
        Duration.ofMillis(c.getLong(CoreConfig.MINING_TARGET_LATENCY_MS)),
        c.getInt(CoreConfig.MINING_MAX_LOGS_PER_STEP),
        Duration.ofMillis(c.getLong(CoreConfig.MINING_SESSION_MAX_AGE_MS)),
        Duration.ofMillis(Math.max(100, c.getLong(CoreConfig.MINING_TARGET_LATENCY_MS) / 4)),
        20,
        c.decodeErrorAction(),
        maxAge <= 0 ? null : Duration.ofMillis(maxAge),
        "discard".equalsIgnoreCase(c.getString(CoreConfig.TRANSACTION_MAX_AGE_ACTION))
            ? MaxAgeAction.DISCARD
            : MaxAgeAction.FAIL,
        LobAssembler.Mode.valueOf(c.lobMode().name()),
        c.getLong(CoreConfig.LOB_MAX_BYTES),
        c.lobOversizeAction() == CoreConfig.LobOversizeAction.FAIL);
  }

  public static EngineSettings defaults() {
    return new EngineSettings(
        Duration.ofSeconds(2),
        8,
        Duration.ofHours(1),
        Duration.ofMillis(500),
        20,
        DecodeErrorAction.FAIL);
  }
}
