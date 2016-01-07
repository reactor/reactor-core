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
package reactor.core.subscription;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;

public final class ScalarSubscription<T> implements Subscription, ReactiveState.Downstream, ReactiveState.Upstream {

	final Subscriber<? super T> actual;

	final T value;

	volatile int once;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<ScalarSubscription> ONCE =
			AtomicIntegerFieldUpdater.newUpdater(ScalarSubscription.class, "once");

	public ScalarSubscription(Subscriber<? super T> actual, T value) {
		this.value = Objects.requireNonNull(value, "value");
		this.actual = Objects.requireNonNull(actual, "actual");
	}

	@Override
	public final Subscriber<? super T> downstream() {
		return actual;
	}

	@Override
	public void request(long n) {
		if (BackpressureUtils.validate(n)) {
			if (ONCE.compareAndSet(this, 0, 1)) {
				Subscriber<? super T> a = actual;
				a.onNext(value);
				a.onComplete();
			}
		}
	}

	@Override
	public void cancel() {
		ONCE.lazySet(this, 1);
	}

	@Override
	public Object upstream() {
		return value;
	}
}
