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
package sh.oso.connect.oracle.journal;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;

/**
 * The default journal reader: a JSON converter that accepts records written with or without the
 * schema envelope, because the task cannot see the worker's {@code schemas.enable} setting. With
 * {@code schemas.enable} set explicitly under {@code cdc.journal.converter.*} it behaves like a
 * plain JsonConverter.
 */
public final class TolerantJsonConverter implements Converter {

  private final JsonConverter withSchemas = new JsonConverter();
  private final JsonConverter withoutSchemas = new JsonConverter();
  private boolean pinned;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Map<String, Object> on = new HashMap<>(configs);
    Map<String, Object> off = new HashMap<>(configs);
    pinned = configs.containsKey("schemas.enable");
    on.putIfAbsent("schemas.enable", "true");
    off.put("schemas.enable", pinned ? configs.get("schemas.enable") : "false");
    withSchemas.configure(on, isKey);
    withoutSchemas.configure(off, isKey);
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    return withSchemas.fromConnectData(topic, schema, value);
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    try {
      return withSchemas.toConnectData(topic, value);
    } catch (DataException e) {
      if (pinned) {
        throw e;
      }
      return withoutSchemas.toConnectData(topic, value);
    }
  }
}
