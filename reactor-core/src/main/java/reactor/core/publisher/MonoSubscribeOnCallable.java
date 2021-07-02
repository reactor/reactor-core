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
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;

/**
 * Executes a Callable and emits its value on the given Scheduler.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
final class MonoSubscribeOnCallable<T> extends Mono<T> implements Fuseable, Scannable{

	final Callable<? extends T> callable;

	final Scheduler scheduler;

	MonoSubscribeOnCallable(Callable<? extends T> callable, Scheduler scheduler) {
		this.callable = Objects.requireNonNull(callable, "callable");
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		FluxSubscribeOnCallable.CallableSubscribeOnSubscription<T> parent =
				new FluxSubscribeOnCallable.CallableSubscribeOnSubscription<>(actual, callable, scheduler);
		actual.onSubscribe(parent);

		try {
			parent.setMainFuture(scheduler.schedule(parent));
		}
		catch (RejectedExecutionException ree) {
			if(parent.state != FluxSubscribeOnCallable.CallableSubscribeOnSubscription.HAS_CANCELLED) {
				actual.onError(Operators.onRejectedExecution(ree, actual.currentContext()));
			}
		}
	}

	@Override
	public Object scanUnsafe(Scannable.Attr key) {
		if (key == Scannable.Attr.RUN_ON) return scheduler;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

		return null;
	}
}
