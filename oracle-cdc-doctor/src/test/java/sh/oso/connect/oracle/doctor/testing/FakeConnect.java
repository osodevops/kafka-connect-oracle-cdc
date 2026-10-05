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
package sh.oso.connect.oracle.doctor.testing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import sh.oso.connect.oracle.doctor.connect.ConnectApi;

/** A Connect worker in memory: one connector, its state and its one offset. */
public final class FakeConnect implements ConnectApi {

  private final List<String> log;
  public final Map<String, String> config = new LinkedHashMap<>();
  public String state = "RUNNING";
  public Map<String, Object> partition = Map.of("server", "cdc");
  public Map<String, Object> offset;
  public Map<String, Object> patched;
  public IOException patchFailure;
  public boolean stopWorks = true;
  public Runnable onStop;
  public final Map<String, List<String>> validateErrors = new HashMap<>();
  public Map<String, String> validated;

  public FakeConnect(List<String> log) {
    this.log = log;
  }

  @Override
  public Map<String, String> config(String connector) {
    return new LinkedHashMap<>(config);
  }

  @Override
  public String state(String connector) {
    return state;
  }

  @Override
  public List<OffsetEntry> offsets(String connector) {
    List<OffsetEntry> out = new ArrayList<>();
    if (offset != null) {
      out.add(new OffsetEntry(partition, new LinkedHashMap<>(offset)));
    }
    return out;
  }

  @Override
  public void patchOffset(String connector, Map<String, Object> p, Map<String, Object> o)
      throws IOException {
    if (!"STOPPED".equals(state)) {
      throw new IOException("HTTP 400: connector " + connector + " is not STOPPED");
    }
    if (patchFailure != null) {
      log.add("patch-failed");
      throw patchFailure;
    }
    log.add("patch");
    patched = new LinkedHashMap<>(o);
    offset = new LinkedHashMap<>(o);
  }

  @Override
  public void stop(String connector) {
    log.add("stop");
    if (onStop != null) {
      onStop.run();
    }
    if (stopWorks) {
      state = "STOPPED";
    }
  }

  @Override
  public void resume(String connector) {
    log.add("resume");
    state = "RUNNING";
  }

  @Override
  public Map<String, List<String>> validate(String connectorClass, Map<String, String> cfg) {
    validated = cfg;
    return validateErrors;
  }
}
