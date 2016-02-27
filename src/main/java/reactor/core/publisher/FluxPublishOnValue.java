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
package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;

/**
 * Publisher indicating a scalar/empty source that subscribes on the specified scheduler.
 * 
 * @param <T>
 */
final class FluxPublishOnValue<T> extends Flux<T> {

	final T value;
	
	final Callable<? extends Consumer<Runnable>> schedulerFactory;

	final boolean eagerCancel;

	public FluxPublishOnValue(T value, 
			Callable<? extends Consumer<Runnable>> schedulerFactory, 
			boolean eagerCancel) {
		this.value = value;
		this.schedulerFactory = Objects.requireNonNull(schedulerFactory, "schedulerFactory");
		this.eagerCancel = eagerCancel;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		Consumer<Runnable> scheduler;
		
		try {
			scheduler = schedulerFactory.call();
		} catch (Throwable e) {
			Exceptions.throwIfFatal(e);
			EmptySubscription.error(s, e);
			return;
		}
		
		if (scheduler == null) {
			EmptySubscription.error(s, new NullPointerException("The schedulerFactory returned a null Function"));
			return;
		}

		if (value == null) {
			FluxPublishOn.ScheduledEmptySubscriptionEager parent =
					new FluxPublishOn.ScheduledEmptySubscriptionEager(s, scheduler);
			s.onSubscribe(parent);
			scheduler.accept(parent);
		}
		else {
			s.onSubscribe(new FluxPublishOn.ScheduledSubscriptionEagerCancel<>(s, value, scheduler));
		}
	}
}
