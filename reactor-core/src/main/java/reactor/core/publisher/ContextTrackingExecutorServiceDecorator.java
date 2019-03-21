/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

import reactor.core.ContextAware;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.scheduler.Scheduler;
import reactor.util.context.Context;

class ContextTrackingExecutorServiceDecorator
		implements BiFunction<Scheduler, ScheduledExecutorService, ScheduledExecutorService> {

	private final Collection<ContextTracker> trackers;

	ContextTrackingExecutorServiceDecorator(Collection<ContextTracker> trackers) {
		this.trackers = trackers;
	}

	@Override
	public ScheduledExecutorService apply(Scheduler scheduler, ScheduledExecutorService service) {
		return new TaskWrappingScheduledExecutorService(service) {

			@Override
			protected Runnable wrap(Runnable runnable) {
				if (!(runnable instanceof ContextAware)) {
					return runnable;
				}
				Context context = ((ContextAware) runnable).currentContext();

				if (!context.hasKey(Hooks.KEY_CONTEXT_TRACKING)) {
					return runnable;
				}

				return new Scheduler.ContextRunnable() {

					@Override
					public Context currentContext() {
						return context;
					}

					@Override
					public void run() {
						Disposable.Composite composite = Disposables.composite();

						for (ContextTracker tracker : trackers) {
							Disposable disposable = tracker.onContextPassing(context);
							composite.add(disposable);
						}

						try {
							runnable.run();
						}
						finally {
							composite.dispose();
						}
					}
				};
			}

			@Override
			protected <V> Callable<V> wrap(Callable<V> callable) {
				if (!(callable instanceof ContextAware)) {
					return callable;
				}
				Context context = ((ContextAware) callable).currentContext();

				if (!context.hasKey(Hooks.KEY_CONTEXT_TRACKING)) {
					return callable;
				}

				return new ContextAwareCallable<>(callable, context);
			}
		};
	}

	private class ContextAwareCallable<V> implements Callable<V>, ContextAware {

		private final Callable<V> callable;
		private final Context context;

		public ContextAwareCallable(Callable<V> callable, Context context) {
			this.callable = callable;
			this.context = context;
		}

		@Override
		public V call() throws Exception {
			Disposable.Composite composite = Disposables.composite();

			for (ContextTracker contextTracker : trackers) {
				Disposable disposable = contextTracker.onContextPassing(context);
				composite.add(disposable);
			}

			try {
				return callable.call();
			}
			finally {
				composite.dispose();
			}
		}

		@Override
		public Context currentContext() {
			return context;
		}
	}
}
