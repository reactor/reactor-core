/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.FluxSubscribeOnValue.ScheduledEmpty;
import reactor.core.publisher.FluxSubscribeOnValue.ScheduledScalar;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

/**
 * Mono indicating a scalar/empty source that subscribes on the specified scheduler.
 *
 * @param <T>
 */
final class MonoSubscribeOnValue<T> extends Mono<T> implements Scannable {

	final T value;

	final Scheduler scheduler;

	MonoSubscribeOnValue(@Nullable T value, Scheduler scheduler) {
		this.value = value;
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		T v = value;
		if (v == null) {
			ScheduledEmpty parent = new ScheduledEmpty(actual);
			actual.onSubscribe(parent);
			try {
				parent.setFuture(scheduler.schedule(parent));
			}
			catch (RejectedExecutionException ree) {
				if (parent.future != OperatorDisposables.DISPOSED) {
					actual.onError(Operators.onRejectedExecution(ree,
							actual.currentContext()));
				}
			}
		}
		else {
			actual.onSubscribe(new ScheduledScalar<>(actual, v, scheduler));
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return scheduler;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

		return null;
	}
}
