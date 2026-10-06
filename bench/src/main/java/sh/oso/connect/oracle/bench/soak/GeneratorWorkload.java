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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import sh.oso.connect.oracle.bench.workload.WorkloadGenerator;
import sh.oso.connect.oracle.bench.workload.WorkloadResult;

/**
 * The seeded {@link WorkloadGenerator} as the soak's workload, run open-ended on its own thread.
 */
public final class GeneratorWorkload implements Soak.Workload {

  private final WorkloadGenerator generator;
  private volatile CompletableFuture<WorkloadResult> run;
  private volatile boolean stopRequested;

  public GeneratorWorkload(WorkloadGenerator generator) {
    this.generator = generator;
  }

  @Override
  public void reset() throws Exception {
    generator.reset();
  }

  @Override
  public void start() {
    run =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return generator.run();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
              } catch (Exception e) {
                throw new CompletionException(e);
              }
            },
            r -> {
              Thread t = new Thread(r, "soak-workload");
              t.setDaemon(true);
              t.start();
            });
  }

  @Override
  public void pause() {
    generator.pause();
  }

  @Override
  public boolean awaitParked(long timeoutMillis) throws InterruptedException {
    return generator.awaitPaused(timeoutMillis);
  }

  @Override
  public void resume() {
    generator.resume();
  }

  @Override
  public void stop() throws Exception {
    stopRequested = true;
    generator.requestStop();
    generator.resume();
    CompletableFuture<WorkloadResult> r = run;
    if (r == null) {
      return;
    }
    try {
      r.get(5, TimeUnit.MINUTES);
    } catch (ExecutionException e) {
      Throwable c = e.getCause();
      if (c instanceof CompletionException && c.getCause() != null) {
        c = c.getCause();
      }
      if (c instanceof Exception ex) {
        throw ex;
      }
      throw e;
    }
  }

  @Override
  public long committed() {
    WorkloadResult p = generator.progress();
    return p == null ? 0 : p.committed.sum();
  }

  @Override
  public long rolledBack() {
    WorkloadResult p = generator.progress();
    return p == null ? 0 : p.rolledBack.sum();
  }

  @Override
  public Throwable failure() {
    CompletableFuture<WorkloadResult> r = run;
    if (r == null || !r.isDone() || stopRequested) {
      return null;
    }
    try {
      r.join();
      return new IllegalStateException("the workload ended before the soak stopped it");
    } catch (CompletionException e) {
      return e.getCause() == null ? e : e.getCause();
    } catch (java.util.concurrent.CancellationException e) {
      return e;
    }
  }
}
