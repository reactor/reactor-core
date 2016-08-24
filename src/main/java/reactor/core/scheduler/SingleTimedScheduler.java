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

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.Cancellation;
import reactor.core.publisher.Operators;
import reactor.util.concurrent.OpenHashSet;

/**
 * A TimedScheduler with an embedded, single-threaded ScheduledExecutorService,
 * shared among all workers.
 */
final class SingleTimedScheduler implements TimedScheduler {

    static final AtomicLong COUNTER = new AtomicLong();
    
    final ScheduledThreadPoolExecutor executor;

    /**
     * Constructs a new SingleTimedScheduler with the given thread factory.
     * @param threadFactory the thread factory to use
     */
    SingleTimedScheduler(ThreadFactory threadFactory) {
        ScheduledThreadPoolExecutor e = (ScheduledThreadPoolExecutor)Executors.newScheduledThreadPool(1, threadFactory);
        e.setRemoveOnCancelPolicy(true);
        executor = e;
    }
    
    @Override
    public Cancellation schedule(Runnable task) {
        try {
            Future<?> f = executor.submit(task);
            return () -> f.cancel(false);
        } catch (RejectedExecutionException ex) {
            return REJECTED;
        }
    }
    
    @Override
    public Cancellation schedule(Runnable task, long delay, TimeUnit unit) {
        try {
            Future<?> f = executor.schedule(task, delay, unit);
            return () -> f.cancel(false);
        } catch (RejectedExecutionException ex) {
            return REJECTED;
        }
    }
    
    @Override
    public Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
        try {
            Future<?> f = executor.scheduleAtFixedRate(task, initialDelay, period, unit);
            return () -> f.cancel(false);
        } catch (RejectedExecutionException ex) {
            return REJECTED;
        }
    }
    
    @Override
    public void start() {
        throw new UnsupportedOperationException("Not supported, yet.");
    }
    
    @Override
    public void shutdown() {
        executor.shutdownNow();
    }
    
    @Override
    public TimedWorker createWorker() {
        return new SingleTimedSchedulerWorker(executor);
    }
    
    static final class SingleTimedSchedulerWorker implements TimedWorker {
        final ScheduledThreadPoolExecutor executor;
        
        OpenHashSet<CancelFuture> tasks;
        
        volatile boolean terminated;
        
        public SingleTimedSchedulerWorker(ScheduledThreadPoolExecutor executor) {
            this.executor = executor;
            this.tasks = new OpenHashSet<>();
        }

        @Override
        public Cancellation schedule(Runnable task) {
            if (terminated) {
                return REJECTED;
            }
            
            TimedScheduledRunnable sr = new TimedScheduledRunnable(task, this);
            
            synchronized (this) {
                if (terminated) {
                    return REJECTED;
                }
                
                tasks.add(sr);
            }
            
            try {
                Future<?> f = executor.submit(sr);
                sr.set(f);
            } catch (RejectedExecutionException ex) {
                sr.dispose();
                return REJECTED;
            }
            
            return sr;
        }
        
        void delete(CancelFuture r) {
            synchronized (this) {
                if (!terminated) {
                    tasks.remove(r);
                }
            }
        }
        
        @Override
        public Cancellation schedule(Runnable task, long delay, TimeUnit unit) {
            if (terminated) {
                return REJECTED;
            }
            
            TimedScheduledRunnable sr = new TimedScheduledRunnable(task, this);
            
            synchronized (this) {
                if (terminated) {
                    return REJECTED;
                }
                
                tasks.add(sr);
            }
            
            try {
                Future<?> f = executor.schedule(sr, delay, unit);
                sr.set(f);
            } catch (RejectedExecutionException ex) {
                sr.dispose();
                return REJECTED;
            }
            
            return sr;
        }
        
        @Override
        public Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
            if (terminated) {
                return REJECTED;
            }
            
            TimedPeriodicScheduledRunnable sr = new TimedPeriodicScheduledRunnable(task, this);
            
            synchronized (this) {
                if (terminated) {
                    return REJECTED;
                }
                
                tasks.add(sr);
            }
            
            try {
                Future<?> f = executor.scheduleAtFixedRate(sr, initialDelay, period, unit);
                sr.set(f);
            } catch (RejectedExecutionException ex) {
                sr.dispose();
                return REJECTED;
            }
            
            return sr;
        }
        
        @Override
        public void shutdown() {
            if (terminated) {
                return;
            }
            terminated = true;
            
            OpenHashSet<CancelFuture> set;
            
            synchronized (this) {
                set = tasks;
                if (set == null) {
                    return;
                }
                tasks = null;
            }
            
            if (!set.isEmpty()) {
                Object[] keys = set.keys();
                for (Object c : keys) {
                    if (c != null) {
                        ((CancelFuture)c).cancelFuture();
                    }
                }
            }
        }
    }

    interface CancelFuture {
        void cancelFuture();
    }
    
    static final class TimedScheduledRunnable
    extends AtomicReference<Future<?>> implements Runnable, Cancellation, CancelFuture {
        /** */
        private static final long serialVersionUID = 2284024836904862408L;
        
        final Runnable task;
        
        final SingleTimedSchedulerWorker parent;
        
        volatile Thread current;
        static final AtomicReferenceFieldUpdater<TimedScheduledRunnable, Thread> CURRENT =
                AtomicReferenceFieldUpdater.newUpdater(TimedScheduledRunnable.class, Thread.class, "current");

        static final Runnable EMPTY = new Runnable() {
            @Override
            public void run() {

            }
        };

        static final Future<?> CANCELLED_FUTURE = new FutureTask<>(EMPTY, null);

        static final Future<?> FINISHED = new FutureTask<>(EMPTY, null);

        public TimedScheduledRunnable(Runnable task, SingleTimedSchedulerWorker parent) {
            this.task = task;
            this.parent = parent;
        }
        
        @Override
        public void run() {
            CURRENT.lazySet(this, Thread.currentThread());
            try {
                try {
                    task.run();
                } catch (Throwable e) {
                    Operators.onErrorDropped(e);
                }
            } finally {
                for (;;) {
                    Future<?> a = get();
                    if (a == CANCELLED_FUTURE) {
                        break;
                    }
                    if (compareAndSet(a, FINISHED)) {
                        if (a != null) {
                            doCancel(a);
                        }
                        parent.delete(this);
                        break;
                    }
                }
                CURRENT.lazySet(this, null);
            }
        }
        
        void doCancel(Future<?> a) {
            a.cancel(Thread.currentThread() != current);
        }
        
        @Override
        public void cancelFuture() {
            for (;;) {
                Future<?> a = get();
                if (a == FINISHED) {
                    return;
                }
                if (compareAndSet(a, CANCELLED_FUTURE)) {
                    if (a != null) {
                        doCancel(a);
                    }
                    return;
                }
            }
        }

        @Override
        public void dispose() {
            for (;;) {
                Future<?> a = get();
                if (a == FINISHED) {
                    return;
                }
                if (compareAndSet(a, CANCELLED_FUTURE)) {
                    if (a != null) {
                        doCancel(a);
                    }
                    parent.delete(this);
                    return;
                }
            }
        }

        
        void setFuture(Future<?> f) {
            for (;;) {
                Future<?> a = get();
                if (a == FINISHED) {
                    return;
                }
                if (a == CANCELLED_FUTURE) {
                    doCancel(a);
                    return;
                }
                if (compareAndSet(null, f)) {
                    return;
                }
            }
        }
        
        @Override
        public String toString() {
            return "TimedScheduledRunnable[cancelled=" + (get() == CANCELLED_FUTURE) +
		            ", task=" + task +
                    "]";
        }
    }

    static final class TimedPeriodicScheduledRunnable
    extends AtomicReference<Future<?>>
    implements Runnable, Cancellation, CancelFuture {
        /** */
        private static final long serialVersionUID = 2284024836904862408L;
        
        final Runnable task;
        
        final SingleTimedSchedulerWorker parent;
        
        volatile Thread current;
        static final AtomicReferenceFieldUpdater<TimedPeriodicScheduledRunnable, Thread> CURRENT =
                AtomicReferenceFieldUpdater.newUpdater(TimedPeriodicScheduledRunnable.class, Thread.class, "current");

        static final Runnable EMPTY = new Runnable() {
            @Override
            public void run() {

            }
        };

        static final Future<?> CANCELLED_FUTURE = new FutureTask<>(EMPTY, null);

        static final Future<?> FINISHED = new FutureTask<>(EMPTY, null);

        public TimedPeriodicScheduledRunnable(Runnable task, SingleTimedSchedulerWorker parent) {
            this.task = task;
            this.parent = parent;
        }
        
        @Override
        public void run() {
            CURRENT.lazySet(this, Thread.currentThread());
            try {
                try {
                    task.run();
                } catch (Throwable ex) {
                    Operators.onErrorDropped(ex);
                    for (;;) {
                        Future<?> a = get();
                        if (a == CANCELLED_FUTURE) {
                            break;
                        }
                        if (compareAndSet(a, FINISHED)) {
                            parent.delete(this);
                            break;
                        }
                    }
                }
            } finally {
                CURRENT.lazySet(this, null);
            }
        }
        
        void doCancel(Future<?> a) {
            a.cancel(false);
        }
        
        @Override
        public void cancelFuture() {
            for (;;) {
                Future<?> a = get();
                if (a == FINISHED) {
                    return;
                }
                if (compareAndSet(a, CANCELLED_FUTURE)) {
                    if (a != null) {
                        doCancel(a);
                    }
                    return;
                }
            }
        }
        
        @Override
        public void dispose() {
            for (;;) {
                Future<?> a = get();
                if (a == FINISHED) {
                    return;
                }
                if (compareAndSet(a, CANCELLED_FUTURE)) {
                    if (a != null) {
                        doCancel(a);
                    }
                    parent.delete(this);
                    return;
                }
            }
        }

        
        void setFuture(Future<?> f) {
            for (;;) {
                Future<?> a = get();
                if (a == FINISHED) {
                    return;
                }
                if (a == CANCELLED_FUTURE) {
                    doCancel(a);
                    return;
                }
                if (compareAndSet(null, f)) {
                    return;
                }
            }
        }
        
        @Override
        public String toString() {
            return "TimedPeriodicScheduledRunnable[cancelled=" + get() + ", task=" + task + "]";
        }
    }

}
