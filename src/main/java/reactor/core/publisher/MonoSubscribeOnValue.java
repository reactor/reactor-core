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

import reactor.core.Cancellation;
import reactor.core.publisher.FluxSubscribeOnValue.*;
import reactor.core.scheduler.Scheduler;

/**
 * Mono indicating a scalar/empty source that subscribes on the specified scheduler.
 * 
 * @param <T>
 */
final class MonoSubscribeOnValue<T> extends Mono<T> {

	final T value;
	
	final Scheduler scheduler;

	public MonoSubscribeOnValue(T value, 
			Scheduler scheduler) {
		this.value = value;
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		T v = value;
		if (v == null) {
			ScheduledEmpty parent = new ScheduledEmpty(s);
			s.onSubscribe(parent);
			Cancellation f = scheduler.schedule(parent);
			if (f == Scheduler.REJECTED) {
				if(parent.future != ScheduledEmpty.CANCELLED) {
					s.onError(Operators.onRejectedExecution());
				}
			}
			else {
				parent.setFuture(f);
			}
		} else {
			s.onSubscribe(new ScheduledScalar<>(s, v, scheduler));
		}
	}
}
