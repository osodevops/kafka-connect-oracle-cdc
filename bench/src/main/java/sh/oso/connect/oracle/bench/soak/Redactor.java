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
package sh.oso.connect.oracle.bench.soak;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Masks secrets in text that comes from outside the soak (exception messages, URLs) before it is
 * logged or written. The soak's own messages never contain a secret, so only foreign text passes
 * through here.
 */
public final class Redactor {

  static final String MASK = "***";

  /** {@code jdbc:oracle:thin:<user>/<password>@...}: credentials inside a thin URL. */
  private static final Pattern THIN_CREDENTIALS =
      Pattern.compile("(?i)(jdbc:oracle:thin:)[^@/\\s]*/[^@\\s]*@");

  /** {@code http://user:password@host}: user information in an HTTP URL. */
  private static final Pattern URL_USERINFO = Pattern.compile("(?i)(https?://)[^/@\\s]+@");

  private final List<String> secrets = new ArrayList<>();

  public Redactor add(String secret) {
    if (secret != null && !secret.isEmpty()) {
      secrets.add(secret);
    }
    return this;
  }

  public String scrub(String text) {
    if (text == null) {
      return null;
    }
    String s = THIN_CREDENTIALS.matcher(text).replaceAll("$1" + MASK + "@");
    s = URL_USERINFO.matcher(s).replaceAll("$1" + MASK + "@");
    for (String secret : secrets) {
      s = s.replace(secret, MASK);
    }
    return s;
  }
}
