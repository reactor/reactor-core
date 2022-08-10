/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.scheduler;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

abstract class RacingDisposeGracefullyStressTest<T extends Scheduler> {

    final T scheduler;
    final CountDownLatch arbiterLatch = new CountDownLatch(1);

    {
        scheduler = createScheduler();
    }

    abstract T createScheduler();
    abstract boolean isTerminated();

    void awaitArbiter() {
        while (true) {
            try {
                if (arbiterLatch.await(40, TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
            catch (InterruptedException ignored) {
            }
        }
    }

    void arbiterStarted() {
        arbiterLatch.countDown();
    }

    boolean checkDisposeGracefullyTimesOut() {
        long start = System.nanoTime();
        try {
            scheduler.disposeGracefully().timeout(Duration.ofMillis(40)).block();
        } catch (Exception e) {
            long duration = System.nanoTime() - start;
            // Validate that the wait took non-zero time.
            return (e.getCause() instanceof TimeoutException) && Duration.ofNanos(duration).toMillis() > 30;
        }
        return false;
    }

    boolean validateSchedulerDisposed() {
        try {
            scheduler.schedule(() -> {});
        } catch (RejectedExecutionException e) {
            scheduler.disposeGracefully().timeout(Duration.ofMillis(50)).block();
            return scheduler.isDisposed() && isTerminated();
        }
        return false;
    }
}
