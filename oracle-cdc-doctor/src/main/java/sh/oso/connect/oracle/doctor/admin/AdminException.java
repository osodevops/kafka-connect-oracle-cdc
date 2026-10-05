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
package sh.oso.connect.oracle.doctor.admin;

/**
 * A refusal or failure of an admin command, with the message the operator sees. Exit code 1 for a
 * refusal or a failed call, 64 for a usage or configuration error.
 */
public final class AdminException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public static final int REFUSED = 1;
  public static final int USAGE = 64;

  private final int exitCode;

  public AdminException(String message) {
    this(REFUSED, message, null);
  }

  public AdminException(int exitCode, String message, Throwable cause) {
    super(message, cause);
    this.exitCode = exitCode;
  }

  public static AdminException usage(String message) {
    return new AdminException(USAGE, message, null);
  }

  public int exitCode() {
    return exitCode;
  }
}
