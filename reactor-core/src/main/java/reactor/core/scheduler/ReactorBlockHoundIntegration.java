/*
 * Copyright (c) 2019-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * {@link BlockHoundIntegration} with Reactor's scheduling mechanism.
 *
 * WARNING: this class is not intended to be public, but {@link java.util.ServiceLoader}
 * requires it to be so. Public visibility DOES NOT make it part of the public API.
 *
 * @since 3.3.0
 */
public final class ReactorBlockHoundIntegration implements BlockHoundIntegration {

    @Override
    public void applyTo(BlockHound.Builder builder) {
        builder.nonBlockingThreadPredicate(current -> current.or(NonBlocking.class::isInstance));

        builder.allowBlockingCallsInside(ScheduledThreadPoolExecutor.class.getName() + "$DelayedWorkQueue", "offer");
        builder.allowBlockingCallsInside(ScheduledThreadPoolExecutor.class.getName() + "$DelayedWorkQueue", "take");
        builder.allowBlockingCallsInside(BoundedElasticScheduler.class.getName() + "$BoundedScheduledExecutorService", "ensureQueueCapacity");

        // Calls ScheduledFutureTask#cancel that may short park in DelayedWorkQueue#remove for getting a lock
        builder.allowBlockingCallsInside(SchedulerTask.class.getName(), "dispose");
        builder.allowBlockingCallsInside(WorkerTask.class.getName(), "dispose");

        builder.allowBlockingCallsInside(ThreadPoolExecutor.class.getName(), "processWorkerExit");

        // Most allowances are from the schedulers package but this one is from the publisher package.
        // For now, let's not add a separate integration, but rather let's define the class name manually
        // ContextRegistry reads files as part of the Service Loader aspect. If class is initialized in a non-blocking thread, BlockHound would complain
        builder.allowBlockingCallsInside("reactor.core.publisher.ContextPropagation", "<clinit>");
        builder.allowBlockingCallsInside(FutureTask.class.getName(),"handlePossibleCancellationInterrupt");
    }
}
