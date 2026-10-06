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

/**
 * See {@link ErrorCode#DECODE}.
 *
 * <p>A message never carries row values: the literal a decode could not read, or the SQL_REDO text
 * around a parse error, is withheld and kept apart, together with the exception that quoted it. The
 * connector shows it only when {@code cdc.log.sensitive.data=true}, through {@link #revealed()}.
 * The decode dead letter queue holds the redo either way.
 */
public final class DecodeException extends OracleCdcException {
  private static final long serialVersionUID = 1L;

  static final String WITHHELD =
      " (the value is withheld; set cdc.log.sensitive.data=true to include it)";

  private final String bare;
  private final String withheld;
  private final String action;
  private final transient Throwable withheldCause;

  public DecodeException(String message, String operatorAction) {
    this(message, null, operatorAction, null, null);
  }

  public DecodeException(String message, String operatorAction, Throwable cause) {
    this(message, null, operatorAction, cause, null);
  }

  private DecodeException(
      String message,
      String withheld,
      String operatorAction,
      Throwable cause,
      Throwable withheldCause) {
    super(ErrorCode.DECODE, withheld == null ? message : message + WITHHELD, operatorAction, cause);
    this.bare = message;
    this.withheld = withheld;
    this.action = operatorAction;
    this.withheldCause = withheldCause;
  }

  /**
   * A decode failure whose detail quotes row data. {@code cause}, whose own message usually quotes
   * the value too, is attached only to the {@link #revealed()} form.
   */
  public static DecodeException withValue(
      String message, String value, String operatorAction, Throwable cause) {
    return new DecodeException(message, value, operatorAction, null, cause);
  }

  /** True when part of the message was withheld. */
  public boolean hasWithheldValue() {
    return withheld != null;
  }

  /** The same failure with the withheld value in the message and its cause attached. */
  public DecodeException revealed() {
    if (withheld == null) {
      return this;
    }
    DecodeException out =
        new DecodeException(bare + ": " + withheld, null, action, withheldCause, null);
    out.setStackTrace(getStackTrace());
    return out;
  }

  /**
   * {@code failure}, or the revealed form when it is a decode failure with a withheld value, or
   * directly caused by one.
   */
  public static Throwable reveal(Throwable failure) {
    if (failure instanceof DecodeException d) {
      return d.revealed();
    }
    if (failure != null && failure.getCause() instanceof DecodeException d && d.withheld != null) {
      return d.revealed();
    }
    return failure;
  }
}
