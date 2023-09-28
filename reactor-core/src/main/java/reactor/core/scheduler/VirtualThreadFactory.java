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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

/**
 * The noop {@link VirtualThread} Reactor {@link ThreadFactory} to be
 * used with {@link ThreadPerTaskBoundedElasticScheduler}, throws exceptions when is being created, so it indicates
 * that current Java Runtime does not support {@link VirtualThread}s.
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
