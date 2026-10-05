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
package sh.oso.connect.oracle.doctor.connect;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The slice of the Kafka Connect REST API the admin commands use (KIP-875 offsets, the stop and
 * resume lifecycle, config validation). {@link HttpConnectApi} is the real one; tests fake it.
 */
public interface ConnectApi {

  /** One stored source offset: the partition map and the offset map. */
  record OffsetEntry(Map<String, Object> partition, Map<String, Object> offset) {}

  /** GET /connectors/{name}/config. */
  Map<String, String> config(String connector) throws IOException;

  /** The connector's state from GET /connectors/{name}/status (RUNNING, PAUSED, STOPPED...). */
  String state(String connector) throws IOException;

  /** GET /connectors/{name}/offsets. */
  List<OffsetEntry> offsets(String connector) throws IOException;

  /** PATCH /connectors/{name}/offsets with one partition; the connector must be STOPPED. */
  void patchOffset(String connector, Map<String, Object> partition, Map<String, Object> offset)
      throws IOException;

  /** PUT /connectors/{name}/stop. */
  void stop(String connector) throws IOException;

  /** PUT /connectors/{name}/resume. */
  void resume(String connector) throws IOException;

  /**
   * PUT /connector-plugins/{class}/config/validate: the error messages per configuration key, for
   * keys that have any.
   */
  Map<String, List<String>> validate(String connectorClass, Map<String, String> config)
      throws IOException;
}
