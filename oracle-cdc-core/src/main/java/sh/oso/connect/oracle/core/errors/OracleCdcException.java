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
package sh.oso.connect.oracle.core.errors;

import java.util.Objects;

/**
 * Base class of every error raised by the capture engine. Carries a stable {@link ErrorCode}, a
 * retriable flag, and an operator action in plain language that the connector surfaces in the
 * Connect task status.
 *
 * <p>Design rule (PRD-00 CORE-ERR): no configuration option ever turns a stop condition into a
 * continue. Subclasses decide only between retry and stop.
 */
public class OracleCdcException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final ErrorCode code;
  private final String operatorAction;

  public OracleCdcException(ErrorCode code, String message, String operatorAction) {
    this(code, message, operatorAction, null);
  }

  public OracleCdcException(
      ErrorCode code, String message, String operatorAction, Throwable cause) {
    super(format(code, message, operatorAction), cause);
    this.code = Objects.requireNonNull(code, "code");
    this.operatorAction = Objects.requireNonNull(operatorAction, "operatorAction");
  }

  public ErrorCode code() {
    return code;
  }

  public boolean retriable() {
    return code.retriable();
  }

  /** What the operator should do next, in plain language. */
  public String operatorAction() {
    return operatorAction;
  }

  public String runbookUrl() {
    return code.runbookUrl();
  }

  private static String format(ErrorCode code, String message, String operatorAction) {
    return "["
        + code.code()
        + "] "
        + message
        + " Operator action: "
        + operatorAction
        + " Runbook: "
        + code.runbookUrl();
  }
}
