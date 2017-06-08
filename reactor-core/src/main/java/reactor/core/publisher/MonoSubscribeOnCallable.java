/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;

import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.scheduler.Scheduler;
import reactor.util.context.Context;


/**
 * Executes a Callable and emits its value on the given Scheduler.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
final class MonoSubscribeOnCallable<T> extends Mono<T> implements Fuseable {

	final Callable<? extends T> callable;

	final Scheduler scheduler;

	MonoSubscribeOnCallable(Callable<? extends T> callable, Scheduler scheduler) {
		this.callable = Objects.requireNonNull(callable, "callable");
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context context) {
		FluxSubscribeOnCallable.CallableSubscribeOnSubscription<T> parent =
				new FluxSubscribeOnCallable.CallableSubscribeOnSubscription<>(s, callable, scheduler);
		s.onSubscribe(parent);

		Disposable f = scheduler.schedule(parent);
		if (f == Scheduler.REJECTED) {
			if(parent.state != FluxSubscribeOnCallable.CallableSubscribeOnSubscription.HAS_CANCELLED) {
				s.onError(Operators.onRejectedExecution());
			}
		}
		else {
			parent.setMainFuture(f);
		}
	}
}
