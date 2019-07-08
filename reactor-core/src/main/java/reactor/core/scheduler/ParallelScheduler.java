/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.scheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import java.util.stream.Stream;

import reactor.core.Disposable;
import reactor.core.Scannable;

/**
 * Scheduler that hosts a fixed pool of single-threaded ScheduledExecutorService-based workers
 * and is suited for parallel work. This scheduler is time-capable (can schedule with
 * delay / periodically).
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 */
final class ParallelScheduler implements Scheduler, Supplier<ScheduledExecutorService>,
                                         Scannable {

    static final AtomicLong COUNTER = new AtomicLong();

    final int n;
    
    final ThreadFactory factory;

    volatile ScheduledExecutorService[] executors;
    static final AtomicReferenceFieldUpdater<ParallelScheduler, ScheduledExecutorService[]> EXECUTORS =
            AtomicReferenceFieldUpdater.newUpdater(ParallelScheduler.class, ScheduledExecutorService[].class, "executors");

    static final ScheduledExecutorService[] SHUTDOWN = new ScheduledExecutorService[0];
    
    static final ScheduledExecutorService TERMINATED;
    static {
        TERMINATED = Executors.newSingleThreadScheduledExecutor();
        TERMINATED.shutdownNow();
    }

    int roundRobin;

    ParallelScheduler(int n, ThreadFactory factory) {
        if (n <= 0) {
            throw new IllegalArgumentException("n > 0 required but it was " + n);
        }
        this.n = n;
        this.factory = factory;
    }

    /**
     * Instantiates the default {@link ScheduledExecutorService} for the ParallelScheduler
     * ({@code Executors.newScheduledThreadPoolExecutor} with core and max pool size of 1).
     */
    @Override
    public ScheduledExecutorService get() {
        ScheduledThreadPoolExecutor poolExecutor = new ScheduledThreadPoolExecutor(1, factory);
        poolExecutor.setMaximumPoolSize(1);
        poolExecutor.setRemoveOnCancelPolicy(true);
        return poolExecutor;
    }
    
	@Override
	public boolean isDisposed() {
		return executors == SHUTDOWN;
	}

	@Override
    public void start() {
        ScheduledExecutorService[] b = null;
        for (;;) {
            ScheduledExecutorService[] a = executors;
            if (a != SHUTDOWN && a != null) {
                if (b != null) {
                    for (ScheduledExecutorService exec : b) {
                        exec.shutdownNow();
                    }
                }
                return;
            }

            if (b == null) {
                b = new ScheduledExecutorService[n];
                for (int i = 0; i < n; i++) {
                    b[i] = Schedulers.decorateExecutorService(this, this.get());
                }
            }
            
            if (EXECUTORS.compareAndSet(this, a, b)) {
                return;
            }
        }
    }

    @Override
    public void dispose() {
        ScheduledExecutorService[] a = executors;
        if (a != SHUTDOWN) {
            a = EXECUTORS.getAndSet(this, SHUTDOWN);
            if (a != SHUTDOWN && a != null) {
                for (ScheduledExecutorService exec : a) {
                    exec.shutdownNow();
                }
            }
        }
    }
    
    ScheduledExecutorService pick() {
        ScheduledExecutorService[] a = executors;
        if (a == null) {
            start();
            a = executors;
            if (a == null) {
                throw new IllegalStateException("executors uninitialized after implicit start()");
            }
        }
        if (a != SHUTDOWN) {
            // ignoring the race condition here, its already random who gets which executor
            int idx = roundRobin;
            if (idx == n) {
                idx = 0;
                roundRobin = 1;
            } else {
                roundRobin = idx + 1;
            }
            return a[idx];
        }
        return TERMINATED;
    }

    @Override
    public Disposable schedule(Runnable task) {
	    return Schedulers.directSchedule(pick(), task, null, 0L, TimeUnit.MILLISECONDS);
    }

    @Override
    public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
	    return Schedulers.directSchedule(pick(), task, null, delay, unit);
    }

    @Override
    public Disposable schedulePeriodically(Runnable task,
            long initialDelay,
            long period,
            TimeUnit unit) {
	    return Schedulers.directSchedulePeriodically(pick(),
			    task,
			    initialDelay,
			    period,
			    unit);
    }

    @Override
    public String toString() {
        StringBuilder ts = new StringBuilder(Schedulers.PARALLEL)
                .append('(').append(n);
        if (factory instanceof ReactorThreadFactory) {
            ts.append(",\"").append(((ReactorThreadFactory) factory).get()).append('\"');
        }
        ts.append(')');
        return ts.toString();
    }

    @Override
    public Object scanUnsafe(Attr key) {
        if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
        if (key == Attr.CAPACITY || key == Attr.BUFFERED) return n; //BUFFERED: number of workers doesn't vary
        if (key == Attr.NAME) return this.toString();

        return null;
    }

    @Override
    public Stream<? extends Scannable> inners() {
        return Stream.of(executors)
                .map(exec -> key -> Schedulers.scanExecutor(exec, key));
    }

    @Override
    public Worker createWorker() {
        return new ExecutorServiceWorker(pick());
    }
}
