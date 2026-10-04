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

import java.util.List;

/** Table metadata the rules inspect, resolved per container from the CDB_ views. */
public record CapturedTable(
    String pdb,
    String owner,
    String name,
    List<Column> columns,
    boolean supplementalAllColumns,
    boolean supplementalPrimaryKey,
    boolean hasPrimaryKey,
    boolean hasNotNullUniqueIndex,
    boolean rowMovement,
    boolean indexOrganized) {

  public record Column(String name, String dataType, boolean identity) {}

  public String fqn() {
    return (pdb == null ? "" : pdb + ".") + owner + "." + name;
  }
}
