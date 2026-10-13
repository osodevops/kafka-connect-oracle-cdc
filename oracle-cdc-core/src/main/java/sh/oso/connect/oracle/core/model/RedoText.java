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
package sh.oso.connect.oracle.core.model;

/**
 * The SQL_REDO and SQL_UNDO text LogMiner gave for a change, kept only when a record format
 * publishes it (the Confluent-compatible format's redo and undo fields, ADR-0028). Never kept for a
 * table that {@code cdc.columns.exclude} could touch, since the text quotes every column's value.
 */
public record RedoText(String redo, String undo) {}
