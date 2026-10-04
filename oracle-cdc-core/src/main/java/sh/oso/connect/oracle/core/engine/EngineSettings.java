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
    DecodeErrorAction onDecodeError) {

  public static EngineSettings from(CoreConfig c) {
    return new EngineSettings(
        Duration.ofMillis(c.getLong(CoreConfig.MINING_TARGET_LATENCY_MS)),
        c.getInt(CoreConfig.MINING_MAX_LOGS_PER_STEP),
        Duration.ofMillis(c.getLong(CoreConfig.MINING_SESSION_MAX_AGE_MS)),
        Duration.ofMillis(Math.max(100, c.getLong(CoreConfig.MINING_TARGET_LATENCY_MS) / 4)),
        20,
        c.decodeErrorAction());
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
