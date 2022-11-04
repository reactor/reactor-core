/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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
import reactor.core.publisher.Mono;

/**
 * Scheduler that hosts a fixed pool of single-threaded ScheduledExecutorService-based workers
 * and is suited for parallel work. This scheduler is time-capable (can schedule with
 * delay / periodically).
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 */
final class ParallelScheduler implements Scheduler, Supplier<ScheduledExecutorService>,
                                         SchedulerState.DisposeAwaiter<ScheduledExecutorService[]>,
                                         Scannable {

    static final ScheduledExecutorService TERMINATED;
    static final ScheduledExecutorService[] SHUTDOWN = new ScheduledExecutorService[0];
    static final AtomicLong COUNTER = new AtomicLong();

    static {
        TERMINATED = Executors.newSingleThreadScheduledExecutor();
        TERMINATED.shutdownNow();
    }

    final int n;
    final ThreadFactory factory;

    volatile SchedulerState<ScheduledExecutorService[]> state;
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<ParallelScheduler, SchedulerState> STATE =
            AtomicReferenceFieldUpdater.newUpdater(
                    ParallelScheduler.class, SchedulerState.class, "state"
            );

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
		SchedulerState<ScheduledExecutorService[]> current = state;
		return current != null && current.currentResource == SHUTDOWN;
	}

	@Override
	public void init() {
		SchedulerState<ScheduledExecutorService[]> a = this.state;
		if (a != null) {
			if (a.currentResource == SHUTDOWN) {
				throw new IllegalStateException(
						"Initializing a disposed scheduler is not permitted"
				);
			}
			// return early - scheduler already initialized
			return;
		}

		SchedulerState<ScheduledExecutorService[]> b =
				SchedulerState.init(new ScheduledExecutorService[n]);

		for (int i = 0; i < n; i++) {
			b.currentResource[i] = Schedulers.decorateExecutorService(this, this.get());
		}

		if (!STATE.compareAndSet(this, null, b)) {
			for (ScheduledExecutorService exec : b.currentResource) {
				exec.shutdownNow();
			}
			if (isDisposed()) {
				throw new IllegalStateException(
						"Initializing a disposed scheduler is not permitted"
				);
			}
		}
	}

	@Override
	public void start() {
		SchedulerState<ScheduledExecutorService[]> a = this.state;

		if (a != null && a.currentResource != SHUTDOWN) {
			return;
		}

		SchedulerState<ScheduledExecutorService[]> b =
				SchedulerState.init(new ScheduledExecutorService[n]);
		for (int i = 0; i < n; i++) {
			b.currentResource[i] = Schedulers.decorateExecutorService(this, this.get());
		}

		if (STATE.compareAndSet(this, a, b)) {
			return;
		}

		// someone else shutdown or started successfully, free the resource
		for (ScheduledExecutorService exec : b.currentResource) {
			exec.shutdownNow();
		}
	}

	@Override
	public boolean await(ScheduledExecutorService[] resource, long timeout, TimeUnit timeUnit) throws InterruptedException {
		for (ScheduledExecutorService executor : resource) {
			if (!executor.awaitTermination(timeout, timeUnit)) {
				return false;
			}
		}
		return true;
	}

    @Override
	public void dispose() {
        SchedulerState<ScheduledExecutorService[]> previous = state;

        if (previous != null && previous.currentResource == SHUTDOWN) {
            if (previous.initialResource != null) {
                for (ScheduledExecutorService executor : previous.initialResource) {
                    executor.shutdownNow();
                }
            }
            return;
        }

        SchedulerState<ScheduledExecutorService[]> shutdown = SchedulerState.transition(
                previous == null ? null : previous.currentResource, SHUTDOWN, this
        );

        STATE.compareAndSet(this, previous, shutdown);

	    // If unsuccessful - either another thread disposed or restarted - no issue,
	    // we only care about the one stored in shutdown.
        if (shutdown.initialResource != null) {
            for (ScheduledExecutorService executor : shutdown.initialResource) {
                executor.shutdownNow();
            }
        }
	}

	@Override
	public Mono<Void> disposeGracefully() {
		return Mono.defer(() -> {
            SchedulerState<ScheduledExecutorService[]> previous = state;

            if (previous != null && previous.currentResource == SHUTDOWN) {
                return previous.onDispose;
            }

            SchedulerState<ScheduledExecutorService[]> shutdown = SchedulerState.transition(
                    previous == null ? null : previous.currentResource, SHUTDOWN, this
            );

            STATE.compareAndSet(this, previous, shutdown);

			// If unsuccessful - either another thread disposed or restarted - no issue,
			// we only care about the one stored in shutdown.
            if (shutdown.initialResource != null) {
                for (ScheduledExecutorService executor : shutdown.initialResource) {
                    executor.shutdown();
                }
            }
            return shutdown.onDispose;
		});
	}

	ScheduledExecutorService pick() {
		SchedulerState<ScheduledExecutorService[]> a = state;
		if (a == null) {
			init();
			a = state;
			if (a == null) {
				throw new IllegalStateException("executors uninitialized after implicit init()");
			}
		}
		if (a.currentResource != SHUTDOWN) {
			// ignoring the race condition here, its already random who gets which executor
			int idx = roundRobin;
			if (idx == n) {
				idx = 0;
				roundRobin = 1;
			}
			else {
				roundRobin = idx + 1;
			}
			return a.currentResource[idx];
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
        return Stream.of(state.currentResource)
                .map(exec -> key -> Schedulers.scanExecutor(exec, key));
    }

    @Override
    public Worker createWorker() {
        return new ExecutorServiceWorker(pick());
    }
}
