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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Loopback;
import reactor.core.flow.Producer;
import reactor.core.state.Completable;
import reactor.core.state.Requestable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.Exceptions;

/**
 * Drops values if the subscriber doesn't request fast enough.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxDrop<T> extends FluxSource<T, T> {

	static final Consumer<Object> NOOP = new Consumer<Object>() {
		@Override
		public void accept(Object t) {

		}
	};

	final Consumer<? super T> onDrop;

	public FluxDrop(Publisher<? extends T> source) {
		super(source);
		this.onDrop = NOOP;
	}


	public FluxDrop(Publisher<? extends T> source, Consumer<? super T> onDrop) {
		super(source);
		this.onDrop = Objects.requireNonNull(onDrop, "onDrop");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new DropSubscriber<>(s, onDrop));
	}

	static final class DropSubscriber<T>
			implements Subscriber<T>, Subscription, Producer, Completable, Requestable, Loopback {

		final Subscriber<? super T> actual;

		final Consumer<? super T> onDrop;

		Subscription s;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<DropSubscriber> REQUESTED =
		  AtomicLongFieldUpdater.newUpdater(DropSubscriber.class, "requested");

		boolean done;

		public DropSubscriber(Subscriber<? super T> actual, Consumer<? super T> onDrop) {
			this.actual = actual;
			this.onDrop = onDrop;
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				BackpressureUtils.addAndGet(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);

				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {

			if (done) {
				try {
					onDrop.accept(t);
				} catch (Throwable e) {
					Exceptions.onNextDropped(t);
				}
				return;
			}

			if (requested != 0L) {

				actual.onNext(t);

				if (requested != Long.MAX_VALUE) {
					REQUESTED.decrementAndGet(this);
				}

			} else {
				try {
					onDrop.accept(t);
				} catch (Throwable e) {
					cancel();
					Exceptions.throwIfFatal(e);
					onError(Exceptions.unwrap(e));
				}
			}
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
		public long requestedFromDownstream() {
			return requested;
		}

		@Override
		public Object connectedInput() {
			return onDrop;
		}
	}
}
