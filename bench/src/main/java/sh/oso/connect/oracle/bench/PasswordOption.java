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
package sh.oso.connect.oracle.bench;

import java.util.function.Function;
import picocli.CommandLine.Option;

/**
 * Exactly one way to give a database password. {@code --password-env} keeps it off the command
 * line, which other users can see and which ends up in shell history, logs and committed scripts.
 */
public final class PasswordOption {

  @Option(
      names = "--password-env",
      paramLabel = "VAR",
      description = "Environment variable holding the password (preferred).")
  String passwordEnv;

  @Option(
      names = "--password",
      description = "Password; prefer --password-env, a command line is visible to other users.")
  String password;

  /** The password, read from the named environment variable when one was given. */
  public String resolve(Function<String, String> env) {
    if (passwordEnv != null) {
      String v = env.apply(passwordEnv);
      if (v == null || v.isEmpty()) {
        throw new IllegalArgumentException(
            "the environment variable " + passwordEnv + " named by --password-env is not set");
      }
      return v;
    }
    return password;
  }
}
