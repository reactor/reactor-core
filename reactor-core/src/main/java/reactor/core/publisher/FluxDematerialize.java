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

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import javax.annotation.Nullable;
import reactor.util.context.Context;

/**
 * @author Stephane Maldini
 */
final class FluxDematerialize<T> extends FluxOperator<Signal<T>, T> {

	FluxDematerialize(Flux<Signal<T>> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super T> subscriber, Context ctx) {
		source.subscribe(new DematerializeSubscriber<>(subscriber), ctx);
	}

	static final class DematerializeSubscriber<T> extends AbstractQueue<T>
			implements InnerOperator<Signal<T>, T>,
			           BooleanSupplier {

		final Subscriber<? super T> actual;

		Subscription s;

		T value;

		boolean done;

		long produced;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<DematerializeSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(DematerializeSubscriber.class,
						"requested");

		volatile boolean cancelled;

		Throwable error;

		DematerializeSubscriber(Subscriber<? super T> subscriber) {
			this.actual = subscriber;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == ThrowableAttr.ERROR) return error;
			if (key == BooleanAttr.CANCELLED) return cancelled;
			if (key == IntAttr.BUFFERED) return size();

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);

				s.request(1);
			}
		}

		@Override
		public void onNext(Signal<T> t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}
			if (t.isOnComplete()) {
				s.cancel();
				onComplete();
			}
			else if (t.isOnError()) {
				s.cancel();
				onError(t.getThrowable());
			}
			else if (t.isOnNext()) {
				T v = value;
				value = t.get();

				if (v != null) {
					produced++;
					actual.onNext(v);
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			done = true;
			error = t;
			long p = produced;
			if (p != 0L) {
				Operators.addAndGet(REQUESTED, this, -p);
			}
			DrainUtils.postCompleteDelayError(actual, this, REQUESTED, this, this, error);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			long p = produced;
			if (p != 0L) {
				Operators.addAndGet(REQUESTED, this, -p);
			}
			DrainUtils.postCompleteDelayError(actual, this, REQUESTED, this, this, error);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (!DrainUtils.postCompleteRequestDelayError(n,
						actual,
						this,
						REQUESTED,
						this,
						this,
						error)) {
					s.request(n);
				}
			}
		}

		@Override
		public void cancel() {
			cancelled = true;
			s.cancel();
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		public boolean getAsBoolean() {
			return cancelled;
		}

		@Override
		public int size() {
			return value == null ? 0 : 1;
		}

		@Override
		public boolean isEmpty() {
			return value == null;
		}

		@Override
		public boolean offer(T e) {
			throw new UnsupportedOperationException();
		}

		@Override
		@Nullable
		public T peek() {
			return value;
		}

		@Override
		@Nullable
		public T poll() {
			T v = value;
			if (v != null) {
				value = null;
				return v;
			}
			return null;
		}

		@Override
		public Iterator<T> iterator() {
			throw new UnsupportedOperationException();
		}
	}
}
