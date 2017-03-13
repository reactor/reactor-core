/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.scheduler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import reactor.core.Cancellation;
import reactor.core.Disposable;
import reactor.util.concurrent.OpenHashSet;

/**
 * Scheduler that hosts a fixed pool of single-threaded ScheduledExecutorService-based workers
 * and is suited for parallel work. This scheduler is time-capable (can schedule with
 * delay / periodically).
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 */
final class ParallelScheduler implements Scheduler, Supplier<ScheduledExecutorService> {

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
        init(n);
    }

    /**
     * Instantiates the default {@link ScheduledExecutorService} for the ParallelScheduler
     * ({@code Executors.newSingleThreadScheduledExecutor}).
     */
    @Override
    public ScheduledExecutorService get() {
        return Executors.newSingleThreadScheduledExecutor(factory);
    }
    
    void init(int n) {
        ScheduledExecutorService[] a = new ScheduledExecutorService[n];
        for (int i = 0; i < n; i++) {
            a[i] = Schedulers.decorateScheduledExecutorService(Schedulers.PARALLEL, this);
        }
        EXECUTORS.lazySet(this, a);
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
            if (a != SHUTDOWN) {
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
                    b[i] = Schedulers.decorateScheduledExecutorService(Schedulers.PARALLEL, this);
                }
            }
            
            if (EXECUTORS.compareAndSet(this, a, b)) {
                return;
            }
        }
    }

    @Override
    public void shutdown() {
        dispose();
    }

    @Override
    public void dispose() {
        ScheduledExecutorService[] a = executors;
        if (a != SHUTDOWN) {
            a = EXECUTORS.getAndSet(this, SHUTDOWN);
            if (a != SHUTDOWN) {
                for (ScheduledExecutorService exec : a) {
                    Schedulers.executorServiceShutdown(exec, Schedulers.PARALLEL);
                }
            }
        }
    }
    
    ScheduledExecutorService pick() {
        ScheduledExecutorService[] a = executors;
        if (a != SHUTDOWN) {
            // ignoring the race condition here, its already random who gets which executor
            int idx = roundRobin;
            if (idx == n) {
                idx = 0;
                roundRobin = 0;
            } else {
                roundRobin = idx + 1;
            }
            return a[idx];
        }
        return TERMINATED;
    }

	@Override
    public Disposable schedule(Runnable task) {
        ScheduledExecutorService exec = pick();
	    try {
		    return new ExecutorServiceScheduler.DisposableFuture(
				    exec.submit(task),
				    false);
	    }
	    catch (RejectedExecutionException ex) {
		    return REJECTED;
	    }
    }
    @Override
    public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
        ScheduledExecutorService exec = pick();
	    try {
		    return new ExecutorServiceScheduler.DisposableFuture(
				    exec.schedule(task, delay, unit),
				    false);
	    }
	    catch (RejectedExecutionException ex) {
		    return REJECTED;
	    }
    }
    @Override
    public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
        ScheduledExecutorService exec = pick();
	    try {
		    return new ExecutorServiceScheduler.DisposableFuture(
				    exec.scheduleAtFixedRate(task, initialDelay, period, unit),
				    false);
	    }
	    catch (RejectedExecutionException ex) {
		    return REJECTED;
	    }
    }

    @Override
    public Worker createWorker() {
        return new ParallelWorker(pick());
    }
    
    static final class ParallelWorker implements Worker {
        final ScheduledExecutorService exec;
        
        OpenHashSet<ParallelWorkerTask> tasks;
        
        volatile boolean shutdown;
        
        public ParallelWorker(ScheduledExecutorService exec) {
            this.exec = exec;
            this.tasks = new OpenHashSet<>();
        }

	    @Override
        public Disposable schedule(Runnable task) {
            if (shutdown) {
                return REJECTED;
            }
            
            ParallelWorkerTask pw = new ParallelWorkerTask(task, this);
            
            synchronized (this) {
                if (shutdown) {
                    return REJECTED;
                }
                tasks.add(pw);
            }
            
            Future<?> f;
            try {
                f = exec.submit(pw);
            } catch (RejectedExecutionException ex) {
                return REJECTED;
            }

            if (shutdown){
            	f.cancel(true);
            	return pw;
            }

            pw.setFuture(f);
            
            return pw;
        }

        @Override
        public Cancellation schedule(Runnable task, long delay, TimeUnit unit) {
            if (shutdown) {
                return REJECTED;
            }

            ParallelWorkerTask pw = new ParallelWorkerTask(task, this);

            synchronized (this) {
                if (shutdown) {
                    return REJECTED;
                }
                tasks.add(pw);
            }

            Future<?> f;
            try {
                f = exec.schedule(pw, delay, unit);
            } catch (RejectedExecutionException ex) {
                return REJECTED;
            }

            if (shutdown){
                f.cancel(true);
                return pw;
            }

            pw.setFuture(f);

            return pw;
        }

        @Override
        public Cancellation schedulePeriodically(Runnable task, long initialDelay,
                long period, TimeUnit unit) {
            if (shutdown) {
                return REJECTED;
            }

            ParallelWorkerTask pw = new ParallelWorkerTask(task, this);

            synchronized (this) {
                if (shutdown) {
                    return REJECTED;
                }
                tasks.add(pw);
            }

            Future<?> f;
            try {
                f = exec.scheduleAtFixedRate(pw, initialDelay, period, unit);
            } catch (RejectedExecutionException ex) {
                return REJECTED;
            }

            if (shutdown){
                f.cancel(true);
                return pw;
            }

            pw.setFuture(f);

            return pw;
        }

        @Override
        public void shutdown() {
            dispose();
        }

        @Override
        public void dispose() {
            if (shutdown) {
                return;
            }
            shutdown = true;
            OpenHashSet<ParallelWorkerTask> set;
            synchronized (this) {
                set = tasks;
                tasks = null;
            }
            
            if (set != null) {
                Object[] a = set.keys();
                for (Object o : a) {
                    if (o != null) {
                        ((ParallelWorkerTask)o).cancelFuture();
                    }
                }
            }
        }

        @Override
        public boolean isDisposed() {
            return shutdown;
        }

        void remove(ParallelWorkerTask task) {
            if (shutdown) {
                return;
            }
            
            synchronized (this) {
                if (shutdown) {
                    return;
                }
                tasks.remove(task);
            }
        }
        
        static final class ParallelWorkerTask implements Runnable, Disposable {
            final Runnable run;
            
            final ParallelWorker parent;
            
            volatile boolean cancelled;
            
            volatile Future<?> future;
            @SuppressWarnings("rawtypes")
            static final AtomicReferenceFieldUpdater<ParallelWorkerTask, Future> FUTURE =
                    AtomicReferenceFieldUpdater.newUpdater(ParallelWorkerTask.class, Future.class, "future");
            
            static final Future<Object> FINISHED = CompletableFuture.completedFuture(null);
            static final Future<Object> CANCELLED = CompletableFuture.completedFuture(null);
            
            public ParallelWorkerTask(Runnable run, ParallelWorker parent) {
                this.run = run;
                this.parent = parent;
            }
            
            @Override
            public void run() {
                if (cancelled || parent.shutdown) {
                    return;
                }
                try {
                    try {
                        run.run();
                    } catch (Throwable ex) {
                        Schedulers.handleError(ex);
                    }
                } finally {
                    for (;;) {
                        Future<?> f = future;
                        if (f == CANCELLED) {
                            break;
                        }
                        if (FUTURE.compareAndSet(this, f, FINISHED)) {
                            parent.remove(this);
                            break;
                        }
                    }
                }
            }

	        @Override
	        public boolean isDisposed() {
		        Future<?> a = future;
		        return FINISHED == a || CANCELLED == a;
	        }

	        @Override
            public void dispose() {
                if (!cancelled) {
                    cancelled = true;
                    
                    Future<?> f = future;
                    if (f != CANCELLED && f != FINISHED) {
                        f = FUTURE.getAndSet(this, CANCELLED);
                        if (f != CANCELLED && f != FINISHED) {
                            if (f != null) {
                                f.cancel(parent.shutdown);
                            }
                            
                            parent.remove(this);
                        }
                    }
                }
            }
            
            void setFuture(Future<?> f) {
                if (future != null || !FUTURE.compareAndSet(this, null, f)) {
                    if (future != FINISHED) {
                        f.cancel(parent.shutdown);
                    }
                }
            }
            
            void cancelFuture() {
                Future<?> f = future;
                if (f != CANCELLED && f != FINISHED) {
                    f = FUTURE.getAndSet(this, CANCELLED);
                    if (f != null && f != CANCELLED && f != FINISHED) {
                        f.cancel(true);
                    }
                }
            }
        }
    }
}
