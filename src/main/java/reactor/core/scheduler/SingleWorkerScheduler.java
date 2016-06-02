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

import java.util.concurrent.Executor;

import reactor.core.flow.Cancellation;

/**
 * Wraps one of the workers of some other Scheduler and
 * provides Worker services on top of it.
 * <p>
 * Use the shutdown() to release the wrapped worker.
 */
final class SingleWorkerScheduler implements Scheduler, Executor {

    final Worker main;
    
    public SingleWorkerScheduler(Scheduler actual) {
        this.main = actual.createWorker();
    }
    
    @Override
    public void shutdown() {
        main.shutdown();
    }
    
    @Override
    public Cancellation schedule(Runnable task) {
        return main.schedule(task);
    }
    
    @Override
    public void execute(Runnable command) {
        main.schedule(command);
    }
    
    @Override
    public Worker createWorker() {
        return new ExecutorScheduler.ExecutorSchedulerWorker(this);
    }
    
}
