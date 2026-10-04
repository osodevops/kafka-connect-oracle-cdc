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
package sh.oso.connect.oracle.core.testkit;

import java.util.ArrayList;
import java.util.List;
import sh.oso.connect.oracle.core.mining.CapturedObject;
import sh.oso.connect.oracle.core.mining.ObjectCatalog;
import sh.oso.connect.oracle.core.mining.ObjectKind;
import sh.oso.connect.oracle.core.model.TableId;

/** Scriptable dictionary objects for resolver tests. */
public final class FakeObjectCatalog implements ObjectCatalog {
  public final List<CapturedObject> objects = new ArrayList<>();

  public FakeObjectCatalog table(int conId, String pdb, String owner, String name, long id) {
    objects.add(
        new CapturedObject(conId, new TableId(pdb, owner, name), id, ObjectKind.TABLE, null));
    return this;
  }

  public FakeObjectCatalog partition(
      int conId, String pdb, String owner, String name, String part, long id) {
    objects.add(
        new CapturedObject(conId, new TableId(pdb, owner, name), id, ObjectKind.PARTITION, part));
    return this;
  }

  public FakeObjectCatalog iotTop(int conId, String pdb, String owner, String name, long id) {
    objects.add(
        new CapturedObject(
            conId, new TableId(pdb, owner, name), id, ObjectKind.IOT_TOP_INDEX, null));
    return this;
  }

  @Override
  public List<CapturedObject> objects() {
    return List.copyOf(objects);
  }
}
