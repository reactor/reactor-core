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
import java.util.function.Predicate;
import javax.annotation.Nullable;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;

/**
 * Filters out values that make a filter function return false.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxFilterFuseable<T> extends FluxOperator<T, T> implements Fuseable {

	final Predicate<? super T> predicate;

	FluxFilterFuseable(Flux<? extends T> source, Predicate<? super T> predicate) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super T> actual) {
		if (actual instanceof ConditionalSubscriber) {
			source.subscribe(new FilterFuseableConditionalSubscriber<>((ConditionalSubscriber<? super T>) actual,
					predicate));
			return;
		}
		source.subscribe(new FilterFuseableSubscriber<>(actual, predicate));
	}

	static final class FilterFuseableSubscriber<T>
			implements InnerOperator<T, T>, QueueSubscription<T>,
			           ConditionalSubscriber<T> {

		final CoreSubscriber<? super T> actual;

		final Predicate<? super T> predicate;

		QueueSubscription<T> s;

		boolean done;

		int sourceMode;

		FilterFuseableSubscriber(CoreSubscriber<? super T> actual,
				Predicate<? super T> predicate) {
			this.actual = actual;
			this.predicate = predicate;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = (QueueSubscription<T>) s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {

			if (sourceMode == ASYNC) {
				actual.onNext(null);
			}
			else {
				if (done) {
					Operators.onNextDropped(t, actual.currentContext());
					return;
				}
				boolean b;

				try {
					b = predicate.test(t);
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t));
					return;
				}
				if (b) {
					actual.onNext(t);
				}
				else {
					s.request(1);
				}
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return false;
			}

			boolean b;

			try {
				b = predicate.test(t);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return false;
			}
			if (b) {
				actual.onNext(t);
				return true;
			}
			return false;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			actual.onComplete();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		@Nullable
		public T poll() {
			if (sourceMode == ASYNC) {
				long dropped = 0;
				for (; ; ) {
					T v = s.poll();

					if (v == null || predicate.test(v)) {
						if (dropped != 0) {
							request(dropped);
						}
						return v;
					}
					dropped++;
				}
			}
			else {
				for (; ; ) {
					T v = s.poll();

					if (v == null || predicate.test(v)) {
						return v;
					}
				}
			}
		}

		@Override
		public boolean isEmpty() {
			return s.isEmpty();
		}

		@Override
		public void clear() {
			s.clear();
		}

		@Override
		public int requestFusion(int requestedMode) {
			int m;
			if ((requestedMode & Fuseable.THREAD_BARRIER) != 0) {
				return Fuseable.NONE;
			}
			else {
				m = s.requestFusion(requestedMode);
			}
			sourceMode = m;
			return m;
		}

		@Override
		public int size() {
			return s.size();
		}
	}

	static final class FilterFuseableConditionalSubscriber<T>
			implements InnerOperator<T, T>, ConditionalSubscriber<T>,
			           QueueSubscription<T> {

		final ConditionalSubscriber<? super T> actual;

		final Predicate<? super T> predicate;

		QueueSubscription<T> s;

		boolean done;

		int sourceMode;

		FilterFuseableConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				Predicate<? super T> predicate) {
			this.actual = actual;
			this.predicate = predicate;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = (QueueSubscription<T>) s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {

			if (sourceMode == ASYNC) {
				actual.onNext(null);
			}
			else {
				if (done) {
					Operators.onNextDropped(t, actual.currentContext());
					return;
				}
				boolean b;

				try {
					b = predicate.test(t);
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t));
					return;
				}
				if (b) {
					actual.onNext(t);
				}
				else {
					s.request(1);
				}
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return false;
			}

			boolean b;

			try {
				b = predicate.test(t);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return false;
			}
			return b && actual.tryOnNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			actual.onComplete();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		@Nullable
		public T poll() {
			if (sourceMode == ASYNC) {
				long dropped = 0;
				for (; ; ) {
					T v = s.poll();

					if (v == null || predicate.test(v)) {
						if (dropped != 0) {
							request(dropped);
						}
						return v;
					}
					dropped++;
				}
			}
			else {
				for (; ; ) {
					T v = s.poll();

					if (v == null || predicate.test(v)) {
						return v;
					}
				}
			}
		}

		@Override
		public boolean isEmpty() {
			return s.isEmpty();
		}

		@Override
		public void clear() {
			s.clear();
		}

		@Override
		public int size() {
			return s.size();
		}

		@Override
		public int requestFusion(int requestedMode) {
			int m;
			if ((requestedMode & Fuseable.THREAD_BARRIER) != 0) {
				return Fuseable.NONE;
			}
			else {
				m = s.requestFusion(requestedMode);
			}
			sourceMode = m;
			return m;
		}
	}

}
