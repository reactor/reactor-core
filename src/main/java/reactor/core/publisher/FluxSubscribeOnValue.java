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

import org.reactivestreams.Subscriber;

import reactor.core.publisher.FluxSubscribeOn.ScheduledEmpty;
import reactor.core.publisher.FluxSubscribeOn.ScheduledScalar;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;

/**
 * Publisher indicating a scalar/empty source that subscribes on the specified scheduler.
 * 
 * @param <T>
 */
final class FluxSubscribeOnValue<T> extends Flux<T> {

	final T value;
	
	final Scheduler scheduler;

	final boolean eagerCancel;

	public FluxSubscribeOnValue(T value, 
			Scheduler scheduler, 
			boolean eagerCancel) {
		this.value = value;
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
		this.eagerCancel = eagerCancel;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		Worker worker;
		
		try {
			worker = scheduler.createWorker();
		} catch (Throwable e) {
			Exceptions.throwIfFatal(e);
			EmptySubscription.error(s, e);
			return;
		}
		
		if (worker == null) {
			EmptySubscription.error(s, new NullPointerException("The scheduler returned a null Function"));
			return;
		}

		T v = value;
		if (v == null) {
			ScheduledEmpty parent = new ScheduledEmpty(s);
			s.onSubscribe(parent);
			Runnable f = scheduler.schedule(parent);
			parent.setFuture(f);
		} else {
			s.onSubscribe(new ScheduledScalar<>(s, v, scheduler));
		}
	}
}
