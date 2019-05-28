/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
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

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * {@link BlockHoundIntegration} with Reactor's scheduling mechanism.
 * Wraps every scheduled {@link Runnable} with a noop {@link Wrapper}, so that it can be
 * detected as an entry point of the non-blocking call stack.
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

        builder.allowBlockingCallsInside("java.util.concurrent.ScheduledThreadPoolExecutor$DelayedWorkQueue", "offer");
        builder.allowBlockingCallsInside(ScheduledThreadPoolExecutor.class.getName() + "$DelayedWorkQueue", "take");

        Schedulers.onScheduleHook("BlockHound", Wrapper::new);
        builder.disallowBlockingCallsInside(Wrapper.class.getName(), "run");
    }

    static final class Wrapper implements Runnable {

        final Runnable delegate;

        Wrapper(Runnable delegate) {
            this.delegate = delegate;
        }

        @Override
        public void run() {
            delegate.run();
        }
    }
}
