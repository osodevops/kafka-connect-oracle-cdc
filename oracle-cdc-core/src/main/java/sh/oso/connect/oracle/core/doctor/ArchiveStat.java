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
package sh.oso.connect.oracle.core.doctor;

import java.time.Instant;

/**
 * One V$ARCHIVED_LOG row with the times and size the sizing rules need (PRD-05 DOC-9, DOC-10,
 * {@code sizing}, {@code redo-profile}). {@code nextTime} is when the log was switched out.
 */
public record ArchiveStat(
    int thread,
    long sequence,
    long firstScn,
    long nextScn,
    Instant firstTime,
    Instant nextTime,
    long bytes,
    boolean deleted) {}
