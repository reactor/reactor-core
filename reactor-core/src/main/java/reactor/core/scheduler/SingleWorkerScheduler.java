/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import reactor.core.Disposable;
import reactor.core.Scannable;

/**
 * Wraps one of the workers of some other Scheduler and
 * provides Worker services on top of it.
 * <p>
 * Use the dispose() to release the wrapped worker.
 * This scheduler is time-capable if the worker itself is time-capable (can schedule with
 * a delay and/or periodically).
 */
final class SingleWorkerScheduler implements Scheduler, Executor, Scannable {

    final Worker main;
    
    SingleWorkerScheduler(Scheduler actual) {
        this.main = actual.createWorker();
    }

    @Override
    public void dispose() {
        main.dispose();
    }

    @Override
    public Disposable schedule(Runnable task) {
        return main.schedule(task);
    }

    @Override
    public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
        return main.schedule(task, delay, unit);
    }

    @Override
    public Disposable schedulePeriodically(Runnable task, long initialDelay,
            long period, TimeUnit unit) {
        return main.schedulePeriodically(task, initialDelay, period, unit);
    }

    @Override
    public void execute(Runnable command) {
        main.schedule(command);
    }
    
    @Override
    public Worker createWorker() {
        return new ExecutorScheduler.ExecutorSchedulerWorker(this);
    }

    @Override
    public boolean isDisposed() {
        return main.isDisposed();
    }

    @Override
    public String toString() {
        Scannable mainScannable = Scannable.from(main);
        if (mainScannable.isScanAvailable()) {
            return Schedulers.SINGLE + "Worker(" + mainScannable.scanUnsafe(Attr.NAME) + ")";
        }
        return Schedulers.SINGLE + "Worker(" + main.toString() + ")";
    }

    @Override
    public Object scanUnsafe(Attr key) {
        if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
        if (key == Attr.PARENT) return main;
        if (key == Attr.NAME) return this.toString();

        return Scannable.from(main).scanUnsafe(key);
    }
}
