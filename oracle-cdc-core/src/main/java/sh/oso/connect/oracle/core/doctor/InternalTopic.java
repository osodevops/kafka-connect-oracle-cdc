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

/**
 * An internal topic the connector writes, as the doctor expects it (PRD-05 DOC-17): the role
 * ({@code ops}, {@code txjournal} and so on), the name after {@code ${prefix}} expansion, whether
 * it must be compacted, and the retention the connector would create it with (-1 for the broker
 * default).
 */
public record InternalTopic(String role, String name, boolean compacted, long retentionMs) {}
