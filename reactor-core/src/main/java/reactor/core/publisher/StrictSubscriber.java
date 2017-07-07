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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.annotation.Nullable;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;

/**
 * @author Stephane Maldini
 */
final class StrictSubscriber<T> implements Scannable, CoreSubscriber<T>, Subscription {

	final Subscriber<? super T> actual;

	volatile Subscription s;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<StrictSubscriber, Subscription>
			S =
			AtomicReferenceFieldUpdater.newUpdater(StrictSubscriber.class,
					Subscription.class,
					"s");

	volatile long requested;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<StrictSubscriber> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(StrictSubscriber.class,
					"requested");

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
	}

	@Override
	public void onNext(T t) {
		actual.onNext(t);
	}

	@Override
	public void onError(Throwable t) {
		actual.onError(t);
	}

	@Override
	public void onComplete() {
		actual.onComplete();
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
				Operators.getAndAddCap(REQUESTED, this, n);
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
		Operators.terminate(S, this);
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == ScannableAttr.PARENT) {
			return s;
		}
		if (key == BooleanAttr.CANCELLED) {
			return s == Operators.cancelledSubscription();
		}
		if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) {
			return requested;
		}
		if (key == ScannableAttr.ACTUAL){
			return actual;
		}

		return null;
	}
}
