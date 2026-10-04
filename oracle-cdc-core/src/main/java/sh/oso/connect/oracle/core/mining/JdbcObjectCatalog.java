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
package sh.oso.connect.oracle.core.mining;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import sh.oso.connect.oracle.core.model.TableId;

/**
 * {@link ObjectCatalog} over CDB_OBJECTS: tables, partitions and subpartitions of non-Oracle users
 * plus the top index of every index-organised table, whose OBJECT_ID is what IOT rows carry as
 * DATA_OBJ# (reference/object-id-stability.md). JDBC-bound; excluded from the unit coverage gate.
 */
public final class JdbcObjectCatalog implements ObjectCatalog {

  private static final String SQL =
      "SELECT o.con_id, c.name, o.owner, o.object_name, o.subobject_name, o.object_id,"
          + " o.object_type FROM cdb_objects o JOIN cdb_users u ON u.username = o.owner AND"
          + " u.con_id = o.con_id LEFT JOIN v$containers c ON c.con_id = o.con_id WHERE"
          + " u.oracle_maintained = 'N' AND o.object_type IN ('TABLE', 'TABLE PARTITION', 'TABLE"
          + " SUBPARTITION') UNION ALL SELECT i.con_id, c.name, i.table_owner, i.table_name, NULL,"
          + " o.object_id, 'IOT TOP' FROM cdb_indexes i JOIN cdb_objects o ON o.con_id = i.con_id"
          + " AND o.owner = i.owner AND o.object_name = i.index_name AND o.object_type = 'INDEX'"
          + " JOIN cdb_users u ON u.username = i.table_owner AND u.con_id = i.con_id LEFT JOIN"
          + " v$containers c ON c.con_id = i.con_id WHERE u.oracle_maintained = 'N' AND"
          + " i.index_type = 'IOT - TOP'";

  private final Connection c;

  public JdbcObjectCatalog(Connection metadataConnection) {
    this.c = metadataConnection;
  }

  @Override
  public List<CapturedObject> objects() throws SQLException {
    List<CapturedObject> out = new ArrayList<>();
    try (Statement s = c.createStatement();
        ResultSet rs = s.executeQuery(SQL)) {
      while (rs.next()) {
        int conId = rs.getInt(1);
        String pdb = rs.getString(2);
        if (conId <= 1 || "CDB$ROOT".equals(pdb)) {
          pdb = conId == 0 ? null : pdb; // non-CDB has con_id 0; root objects are never captured
          if (conId == 1) {
            continue;
          }
        }
        String type = rs.getString(7);
        ObjectKind kind =
            switch (type) {
              case "TABLE" -> ObjectKind.TABLE;
              case "TABLE PARTITION" -> ObjectKind.PARTITION;
              case "TABLE SUBPARTITION" -> ObjectKind.SUBPARTITION;
              default -> ObjectKind.IOT_TOP_INDEX;
            };
        out.add(
            new CapturedObject(
                conId,
                new TableId(pdb, rs.getString(3), rs.getString(4)),
                rs.getLong(6),
                kind,
                rs.getString(5)));
      }
    }
    return out;
  }
}
