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
 * ROWIDs as the buffer sees them (ADR-0015). LogMiner names a row with the all-A placeholder
 * whenever its statement writes an out-of-row LOB, while the undo rows of that statement carry the
 * real ROWID. The engine therefore gives such a change a synthetic ROWID, unique in its
 * transaction, which undo matching can target and which is never published:
 *
 * <ul>
 *   <li>{@code <real or placeholder>#R<n>}: a row piece (INSERT or UPDATE) with its LOB writes;
 *   <li>{@code <real or placeholder>#L<n>}: LOB writes without a row piece, whose call boundaries
 *       LogMiner does not show, so an undo downgrades it instead of removing it;
 *   <li>{@code <real or placeholder>#X<n>}: such a downgraded change, which no undo matches.
 * </ul>
 */
public final class RowIds {

  public static final String PLACEHOLDER = "AAAAAAAAAAAAAAAAAA";

  private RowIds() {}

  public static boolean isPlaceholder(String rowId) {
    return PLACEHOLDER.equals(rowId);
  }

  public static String rowPiece(String rowId, String unique) {
    return base(rowId) + "#R" + unique;
  }

  public static String lobGroup(String rowId, String unique) {
    return base(rowId) + "#L" + unique;
  }

  /** The downgraded form of a LOB group's ROWID. */
  public static String inert(String lobGroupRowId) {
    int i = lobGroupRowId.indexOf('#');
    return lobGroupRowId.substring(0, i) + "#X" + lobGroupRowId.substring(i + 2);
  }

  public static boolean isSynthetic(String rowId) {
    return rowId != null && rowId.indexOf('#') > 0;
  }

  public static boolean isLobGroup(String rowId) {
    return kind(rowId) == 'L';
  }

  public static boolean isInert(String rowId) {
    return kind(rowId) == 'X';
  }

  /** The real ROWID inside a synthetic one, or null when LogMiner only gave the placeholder. */
  public static String real(String rowId) {
    if (rowId == null) {
      return null;
    }
    int i = rowId.indexOf('#');
    String r = i > 0 ? rowId.substring(0, i) : rowId;
    return isPlaceholder(r) ? null : r;
  }

  private static char kind(String rowId) {
    if (rowId == null) {
      return 0;
    }
    int i = rowId.indexOf('#');
    return i > 0 && i + 1 < rowId.length() ? rowId.charAt(i + 1) : 0;
  }

  private static String base(String rowId) {
    return rowId == null ? PLACEHOLDER : rowId;
  }
}
