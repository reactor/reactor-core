/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscription;

/**
 * A {@link Callable} derived from a {@link Mono} that will both {@link Mono#subscribe()}
 * to the Mono and {@link #blockingGet() block} until the Mono is fulfilled when
 * triggering the {@link #call()}.
 * <p>
 * Note that since the source is expected to be always a Mono, the upstream isn't
 * {@link Subscription#cancel() cancelled} during onNext.
 *
 * @author Simon Basle
 */
final class MonoToCallable<T> extends BlockingSingleSubscriber<T> implements Callable<T> {

	final Mono<T> upstream;

	volatile int once;
	static final AtomicIntegerFieldUpdater<MonoToCallable> ONCE =
			AtomicIntegerFieldUpdater.newUpdater(MonoToCallable.class, "once");

	MonoToCallable(Mono<T> upstream) {
		super();
		this.upstream = upstream;
	}

	@Override
	public T call() throws Exception {
		if (ONCE.compareAndSet(this, 0, 1)) {
			upstream.subscribe(this);
		}
		return blockingGet();
	}

	@Override
	public void onNext(T t) {
		if (value == null) {
			value = t;
			//we don't dispose as this operator is expected to only be used with Mono
			countDown();
		}
	}

	@Override
	public void onError(Throwable t) {
		if (value == null) {
			error = t;
		}
		countDown();
	}
}
