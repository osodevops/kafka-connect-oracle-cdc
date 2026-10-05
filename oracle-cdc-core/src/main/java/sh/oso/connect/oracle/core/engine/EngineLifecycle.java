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
package sh.oso.connect.oracle.core.engine;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Runs a {@link CaptureEngine} on its own thread: step after step, sleeping the idle poll when the
 * database is quiet, until stopped. The first {@link OracleCdcException} ends the loop and is kept
 * for the owner to rethrow from poll(); the owner closes the sources.
 */
public final class EngineLifecycle implements AutoCloseable {

  private final CaptureEngine engine;
  private final Duration idlePoll;
  private final Consumer<Throwable> onFailure;
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private final CountDownLatch stopped = new CountDownLatch(1);
  private volatile boolean running;
  private volatile Thread thread;

  public EngineLifecycle(CaptureEngine engine, Duration idlePoll, Consumer<Throwable> onFailure) {
    this.engine = engine;
    this.idlePoll = idlePoll;
    this.onFailure = onFailure;
  }

  public synchronized void start(String threadName) {
    if (thread != null) {
      throw new IllegalStateException("already started");
    }
    running = true;
    thread = new Thread(this::loop, threadName);
    thread.setDaemon(true);
    thread.start();
  }

  private void loop() {
    try {
      while (running) {
        CaptureEngine.Progress p = engine.runOnce();
        if (p == CaptureEngine.Progress.IDLE) {
          Thread.sleep(idlePoll.toMillis());
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (SQLException | RuntimeException e) {
      // the callback runs before the failure becomes visible, so anything it queues (the ops stop
      // event) is drained by the task before poll() rethrows
      try {
        onFailure.accept(e);
      } finally {
        failure.set(e);
      }
    } finally {
      running = false;
      stopped.countDown();
    }
  }

  /** The error that ended the loop, or null while it runs or after a clean stop. */
  public Throwable failure() {
    return failure.get();
  }

  public boolean isRunning() {
    return running;
  }

  /** Stops within {@code deadline}; interrupts the step if it does not end on its own. */
  public boolean stop(Duration deadline) throws InterruptedException {
    running = false;
    Thread t = thread;
    if (t == null) {
      return true;
    }
    if (!stopped.await(deadline.toMillis() / 2, TimeUnit.MILLISECONDS)) {
      t.interrupt();
    }
    return stopped.await(deadline.toMillis() / 2, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() throws InterruptedException {
    stop(Duration.ofSeconds(30));
  }
}
