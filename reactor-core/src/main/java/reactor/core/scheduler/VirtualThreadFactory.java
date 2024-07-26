/*
 * Copyright (c) 2018-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.concurrent.ThreadFactory;
import java.util.function.BiConsumer;

import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

/**
 * The noop {@link VirtualThread} Reactor {@link ThreadFactory} to be
 * used with {@link BoundedElasticThreadPerTaskScheduler}.
 * This {@link VirtualThreadFactory} variant is included when Reactor is used with
 * JDK versions lower than 21,
 * and all methods raise an {@link UnsupportedOperationException}.
 * An alternative variant is available for use on JDK 21+
 * where virtual threads are supported.
 *
 * @author Oleh Dokuka
 */
class VirtualThreadFactory implements ThreadFactory,
                                      Thread.UncaughtExceptionHandler {


	VirtualThreadFactory(String name,
			boolean inheritThreadLocals,
			@Nullable BiConsumer<Thread, Throwable> uncaughtExceptionHandler) {
		throw new UnsupportedOperationException("Virtual Threads are not supported in JVM lower than 21");
	}

	@Override
	public final Thread newThread(@NonNull Runnable runnable) {
		throw new UnsupportedOperationException("Virtual Threads are not supported in JVM lower than 21");
	}

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		throw new UnsupportedOperationException("Virtual Threads are not supported in JVM lower than 21");
	}
}
