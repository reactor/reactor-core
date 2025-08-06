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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The {@link VirtualThread} Reactor {@link ThreadFactory} to be used with
 * {@link BoundedElasticThreadPerTaskScheduler}, delegates all allocations to real {@link
 * java.lang.ThreadBuilders.VirtualThreadFactory}
 *
 * @author Oleh Dokuka
 */
class VirtualThreadFactory implements ThreadFactory,
                                      Thread.UncaughtExceptionHandler {

	final ThreadFactory                         delegate;
	@Nullable
	final BiConsumer<Thread, Throwable> uncaughtExceptionHandler;

	VirtualThreadFactory(String name,
			boolean inheritThreadLocals,
			@Nullable BiConsumer<Thread, Throwable> uncaughtExceptionHandler) {
		this.uncaughtExceptionHandler = uncaughtExceptionHandler;
		this.delegate = Thread.ofVirtual()
		      .name(name, 1)
		      .uncaughtExceptionHandler(this)
			  .inheritInheritableThreadLocals(inheritThreadLocals)
		      .factory();
	}

	@Override
	public final Thread newThread(@NonNull Runnable runnable) {
		return delegate.newThread(runnable);
	}

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		if (uncaughtExceptionHandler == null) {
			return;
		}

		uncaughtExceptionHandler.accept(t,e);
	}
}
