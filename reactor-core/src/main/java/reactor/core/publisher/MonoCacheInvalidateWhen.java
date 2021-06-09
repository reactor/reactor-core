/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * @author Simon Baslé
 */
final class MonoCacheInvalidateWhen<T> extends InternalMonoOperator<T, T> {

	final Function<? super T, Mono<Void>> invalidationTriggerGenerator;

	volatile InvalidateWhenCoordinator<T> coordinator;
	static final AtomicReferenceFieldUpdater<MonoCacheInvalidateWhen, InvalidateWhenCoordinator> COORDINATOR =
			AtomicReferenceFieldUpdater.newUpdater(MonoCacheInvalidateWhen.class, InvalidateWhenCoordinator.class, "coordinator");

	MonoCacheInvalidateWhen(Mono<T> source, Function<? super T, Mono<Void>> invalidationTriggerGenerator) {
		super(source);
		this.invalidationTriggerGenerator = Objects.requireNonNull(invalidationTriggerGenerator, "invalidationTriggerGenerator");
	}

	@Nullable
	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) throws Throwable {
		InvalidateWhenCoordinator<T> c;
		for(;;) {
			c = this.coordinator;
			if (c == null) {
				c = new InvalidateWhenCoordinator<>();
				if (COORDINATOR.compareAndSet(this, null, c)) {
					break;
				}
			}
		}
		c.add(actual);
		return null;
	}

	/**
	 * The coordinator that keeps track of connection state, actually caches the value, tracks downstream subscribers
	 * that come before resolution, etc...
	 * @param <T> the type of cached value
	 */
	static final class InvalidateWhenCoordinator<T> extends AtomicReference<T> implements InnerOperator<T, T> {

		Subscription s;

		boolean add(CoreSubscriber<? super T> subscriber) {
			T cached = get();
			if (cached != null) {
				Operators.terminate()
			}
		}

	}

	/**
	 * The subscriber that watches the trigger and propagates that to the coordinator.
	 */
	static final class InvalidateTriggerSubscriber implements InnerConsumer<Void> {

	}



}
