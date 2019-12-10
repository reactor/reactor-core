/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Reactive Streams Commons safe exit
 */
final class StrictSubscriber<T> implements Scannable, CoreSubscriber<T>, Subscription {

	final Subscriber<? super T> actual;

	volatile Subscription s;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<StrictSubscriber, Subscription> S =
			AtomicReferenceFieldUpdater.newUpdater(StrictSubscriber.class,
					Subscription.class,
					"s");

	volatile long requested;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<StrictSubscriber> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(StrictSubscriber.class, "requested");

	volatile int wip;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<StrictSubscriber> WIP =
			AtomicIntegerFieldUpdater.newUpdater(StrictSubscriber.class, "wip");

	volatile Throwable error;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<StrictSubscriber, Throwable> ERROR =
			AtomicReferenceFieldUpdater.newUpdater(StrictSubscriber.class,
					Throwable.class,
					"error");

	volatile boolean done;

	StrictSubscriber(Subscriber<? super T> actual) {
		this.actual = actual;
	}

	@Override
	public void onSubscribe(Subscription s) {
		if (Operators.validate(this.s, s)) {

			actual.onSubscribe(this);

			if (Operators.setOnce(S, this, s)) {
				long r = REQUESTED.getAndSet(this, 0L);
				if (r != 0L) {
					s.request(r);
				}
			}
		}
		else {
			onError(new IllegalStateException("§2.12 violated: onSubscribe must be called at most once"));
		}
	}

	@Override
	public void onNext(T t) {
		if (WIP.get(this) == 0 && WIP.compareAndSet(this, 0, 1)) {
			actual.onNext(t);
			if (WIP.decrementAndGet(this) != 0) {
				Throwable ex = Exceptions.terminate(ERROR, this);
				if (ex != null) {
					actual.onError(ex);
				} else {
					actual.onComplete();
				}
			}
		}
	}

	@Override
	public void onError(Throwable t) {
		done = true;
		if (Exceptions.addThrowable(ERROR, this, t)) {
			if (WIP.getAndIncrement(this) == 0) {
				actual.onError(Exceptions.terminate(ERROR, this));
			}
		}
		else {
			Operators.onErrorDropped(t, Context.empty());
		}
	}

	@Override
	public void onComplete() {
		done = true;
		if (WIP.getAndIncrement(this) == 0) {
			Throwable ex = Exceptions.terminate(ERROR, this);
			if (ex != null) {
				actual.onError(ex);
			}
			else {
				actual.onComplete();
			}
		}
	}

	@Override
	public void request(long n) {
		if (n <= 0) {
			cancel();
			onError(new IllegalArgumentException(
					"§3.9 violated: positive request amount required but it was " + n));
			return;
		}
		Subscription a = s;
		if (a != null) {
			a.request(n);
		}
		else {
			Operators.addCap(REQUESTED, this, n);
			a = s;
			if (a != null) {
				long r = REQUESTED.getAndSet(this, 0L);
				if (r != 0L) {
					a.request(n);
				}
			}
		}
	}

	@Override
	public void cancel() {
		if(!done) {
			Operators.terminate(S, this);
		}
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) {
			return s;
		}
		if (key == Attr.CANCELLED) {
			return s == Operators.cancelledSubscription();
		}
		if (key == Attr.REQUESTED_FROM_DOWNSTREAM) {
			return requested;
		}
		if (key == Attr.ACTUAL) {
			return actual;
		}

		return null;
	}

	@Override
	public Context currentContext() {
		return Context.empty();
	}
}
