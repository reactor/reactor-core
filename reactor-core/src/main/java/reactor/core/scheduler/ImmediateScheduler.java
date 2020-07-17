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

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Scannable;

/**
 * Executes tasks on the caller's thread immediately.
 * <p>
 * Use the ImmediateScheduler.instance() to get a shared, stateless instance of this scheduler.
 * This scheduler is NOT time-capable (can't schedule with delay / periodically).
 *
 * @author Stephane Maldini
 */
final class ImmediateScheduler implements Scheduler, Scannable {

    private static final ImmediateScheduler INSTANCE;

    static {
        INSTANCE = new ImmediateScheduler();
        INSTANCE.start();
    }

    public static Scheduler instance() {
        return INSTANCE;
    }
    
    private ImmediateScheduler() {
    }
    
    static final Disposable FINISHED = Disposables.disposed();
    
    @Override
    public Disposable schedule(Runnable task) {
        task.run();
        return FINISHED;
    }

    @Override
    public void dispose() {
        //NO-OP
    }


    @Override
    public Object scanUnsafe(Attr key) {
        if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
        if (key == Attr.NAME) return Schedulers.IMMEDIATE;

        return null;
    }

    @Override
    public Worker createWorker() {
        return new ImmediateSchedulerWorker();
    }

    static final class ImmediateSchedulerWorker implements Scheduler.Worker, Scannable {
        
        volatile boolean shutdown;

        @Override
        public Disposable schedule(Runnable task) {
            if (shutdown) {
                throw Exceptions.failWithRejected();
            }
            task.run();
            return FINISHED;
        }

        @Override
        public void dispose() {
            shutdown = true;
        }

        @Override
        public boolean isDisposed() {
            return shutdown;
        }

        @Override
        public Object scanUnsafe(Attr key) {
            if (key == Attr.TERMINATED || key == Attr.CANCELLED) return shutdown;
            if (key == Attr.NAME) return Schedulers.IMMEDIATE + ".worker";

            return null;
        }
    }

}
