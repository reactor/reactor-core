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
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Fuseable;
import reactor.core.flow.Fuseable.ConditionalSubscriber;
import reactor.core.flow.Loopback;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.publisher.FluxFilterFuseable.FilterFuseableConditionalSubscriber;
import reactor.core.publisher.FluxFilterFuseable.FilterFuseableSubscriber;
import reactor.core.state.Completable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.Exceptions;

/**
 * Filters out values that make a filter function return false.
 *
 * @param <T> the value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @since 2.5
 */
final class FluxFilter<T> extends FluxSource<T, T> {

	final Predicate<? super T> predicate;

	public FluxFilter(Publisher<? extends T> source, Predicate<? super T> predicate) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
	}

	public Predicate<? super T> predicate() {
		return predicate;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (source instanceof Fuseable) {
			if (s instanceof ConditionalSubscriber) {
				source.subscribe(new FilterFuseableConditionalSubscriber<>((ConditionalSubscriber<? super T>) s,
						predicate));
				return;
			}
			source.subscribe(new FilterFuseableSubscriber<>(s, predicate));
			return;
		}
		if (s instanceof ConditionalSubscriber) {
			source.subscribe(new FilterConditionalSubscriber<>((ConditionalSubscriber<? super T>)s, predicate));
			return;
		}
		source.subscribe(new FilterSubscriber<>(s, predicate));
	}

	static final class FilterSubscriber<T>
	implements Receiver, Producer, Loopback, Completable, Subscription, ConditionalSubscriber<T> {
		final Subscriber<? super T> actual;

		final Predicate<? super T> predicate;

		Subscription s;

		boolean done;

		public FilterSubscriber(Subscriber<? super T> actual, Predicate<? super T> predicate) {
			this.actual = actual;
			this.predicate = predicate;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}

			boolean b;

			try {
				b = predicate.test(t);
			} catch (Throwable e) {
				Exceptions.throwIfFatal(e);
				s.cancel();

				Exceptions.throwIfFatal(e);
				onError(Exceptions.unwrap(e));
				return;
			}
			if (b) {
				actual.onNext(t);
			} else {
				s.request(1);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return false;
			}

			boolean b;

			try {
				b = predicate.test(t);
			} catch (Throwable e) {
				Exceptions.throwIfFatal(e);
				s.cancel();

				onError(Exceptions.unwrap(e));
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
				Exceptions.onErrorDropped(t);
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
		public boolean isStarted() {
			return s != null && !done;
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object connectedInput() {
			return predicate;
		}

		@Override
		public Object upstream() {
			return s;
		}
		
		@Override
		public void request(long n) {
			s.request(n);
		}
		
		@Override
		public void cancel() {
			s.cancel();
		}
	}

	static final class FilterConditionalSubscriber<T>
	implements Receiver, Producer, Loopback, Completable, Subscription, ConditionalSubscriber<T> {
		final ConditionalSubscriber<? super T> actual;

		final Predicate<? super T> predicate;

		Subscription s;

		boolean done;

		public FilterConditionalSubscriber(ConditionalSubscriber<? super T> actual, Predicate<? super T> predicate) {
			this.actual = actual;
			this.predicate = predicate;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}

			boolean b;

			try {
				b = predicate.test(t);
			} catch (Throwable e) {
				Exceptions.throwIfFatal(e);
				s.cancel();
				onError(Exceptions.unwrap(e));
				return;
			}
			if (b) {
				actual.onNext(t);
			} else {
				s.request(1);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return false;
			}

			boolean b;

			try {
				b = predicate.test(t);
			} catch (Throwable e) {
				Exceptions.throwIfFatal(e);
				s.cancel();
				onError(Exceptions.unwrap(e));
				return false;
			}
			if (b) {
				return actual.tryOnNext(t);
			}
			return false;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
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
		public boolean isStarted() {
			return s != null && !done;
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object connectedInput() {
			return predicate;
		}

		@Override
		public Object upstream() {
			return s;
		}
		
		@Override
		public void request(long n) {
			s.request(n);
		}
		
		@Override
		public void cancel() {
			s.cancel();
		}
	}

}
