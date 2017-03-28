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

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Scannable;

/**
 * Maps the upstream value into a single {@code true} or {@code false} value
 * provided by a generated Publisher for that input value and emits the input value if
 * the inner Publisher returned {@code true}.
 * <p>
 * Only the first item emitted by the inner Publisher's are considered. If
 * the inner Publisher is empty, no resulting item is generated for that input value.
 *
 * @param <T> the input value type
 * @author Simon Baslé
 */
class MonoFilterWhen<T> extends MonoSource<T, T> {

	final Function<? super T, ? extends Publisher<Boolean>> asyncPredicate;

	MonoFilterWhen(Publisher<T> source,
			Function<? super T, ? extends Publisher<Boolean>> asyncPredicate) {
		super(source);
		this.asyncPredicate = asyncPredicate;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new MonoFilterWhenSubscriber<>(s, asyncPredicate));
	}

	static final class MonoFilterWhenSubscriber<T> extends Operators.MonoSubscriber<T, T> {

		final Function<? super T, ? extends Publisher<Boolean>> asyncPredicate;

		Subscription upstream;

		volatile FilterWhenInner asyncFilter;
		volatile Throwable       error;
		volatile int             filterState;

		static final AtomicReferenceFieldUpdater<MonoFilterWhenSubscriber, FilterWhenInner> ASYNC_FILTER =
				AtomicReferenceFieldUpdater.newUpdater(MonoFilterWhenSubscriber.class, FilterWhenInner.class, "asyncFilter");
		
		static final AtomicReferenceFieldUpdater<MonoFilterWhenSubscriber, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(MonoFilterWhenSubscriber.class, Throwable.class, "error");

		static final FilterWhenInner INNER_CANCELLED = new FilterWhenInner(null, false);

		static final int STATE_FRESH         = 0; //the source value is fresh
		static final int STATE_RUNNING       = 1; //the filter is running asynchronously
		static final int STATE_RESULT        = 2; //the output Mono is resolved

		MonoFilterWhenSubscriber(Subscriber<? super T> actual, Function<? super T, ? extends Publisher<Boolean>> asyncPredicate) {
			super(actual);
			this.asyncPredicate = asyncPredicate;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(upstream, s)) {
				upstream = s;
				actual.onSubscribe(this);
				s.request(1);
			}
		}

		@Override
		public void onNext(T t) {
			if (filterState == STATE_FRESH) {
				setValue(t);
				Publisher<Boolean> p;

				try {
					p = Objects.requireNonNull(asyncPredicate.apply(t),
							"The asyncPredicate returned a null value");
				}
				catch (Throwable ex) {
					Exceptions.throwIfFatal(ex);
						//TODO not really needed as we don't expect to get there if the Mono source errors
					Exceptions.addThrowable(ERROR, this, ex);
					filterState = STATE_RESULT;
					super.onError(error);
					return;
				}

				if (p instanceof Callable) {
					Boolean u;

					try {
						u = ((Callable<Boolean>) p).call();
					}
					catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						//TODO not really needed as we don't expect to get there if the Mono source errors
						Exceptions.addThrowable(ERROR, this, ex);
						filterState = STATE_RESULT;
						super.onError(error);
						return;
					}

					if (u != null && u) {
						actual.onNext(t);
					}
				}
				else {
					FilterWhenInner inner = new FilterWhenInner(this, !(p instanceof Mono));
					if (ASYNC_FILTER.compareAndSet(this, null, inner)) {
						filterState = STATE_RUNNING;
						p.subscribe(inner);
					}
				}
			}
			//TODO drop?
		}

		@Override
		public void onComplete() {
			if (filterState == STATE_FRESH) {
				//there was no value, we can complete empty
				super.onComplete();
			}
			//otherwise just wait for the inner filter to apply, rather than complete too soon
		}

		@Override
		public void onError(Throwable t) {
			Exceptions.addThrowable(ERROR, this, t);
			if (filterState != STATE_RUNNING) {
				super.onError(t);
			}
		}

		@Override
		public Throwable getError() {
			return error;
		}

		@Override
		public void cancel() {
			if (super.state != CANCELLED) {
				super.cancel();
				upstream.cancel();
				cancelInner();
			}
		}

		void cancelInner() {
			FilterWhenInner a = asyncFilter;
			if (a != INNER_CANCELLED) {
				a = ASYNC_FILTER.getAndSet(this, INNER_CANCELLED);
				if (a != null && a != INNER_CANCELLED) {
					a.cancel();
				}
			}
		}

		void innerResult(Boolean item) {
			filterState = STATE_RESULT;
			if (item != null && item) {
				//will reset the value with itself, but using parent's `value` saves a field
				complete(value);
			}
			else {
				super.onComplete();
			}
		}

		void innerError(Throwable ex) {
			Exceptions.addThrowable(ERROR, this, ex);
			super.onError(error);
		}

		void innerComplete() {
			if (filterState == STATE_RUNNING) {
				Throwable e = error;
				if (e == null) {
					super.onComplete();
				}
				else {
					super.onError(e);
				}
			}
		}

		@Override
		public Object upstream() {
			return upstream;
		}

		@Override
		public Object scan(Attr key) {
			switch (key) {
				case PARENT:
					return upstream;
				case CANCELLED:
					return super.state == CANCELLED;
				case ERROR:
					return error;
				case BUFFERED:
					return filterState == STATE_RUNNING ? 1 : 0;
				case PREFETCH:
					return 1;
				default:
					return super.scan(key);
			}
		}

		@Override
		public Stream<? extends Scannable> inners() {
			FilterWhenInner c = asyncFilter;
			return c == null ? Stream.empty() : Stream.of(c);
		}
	}

	static final class FilterWhenInner implements InnerConsumer<Boolean> {

		final MonoFilterWhenSubscriber<?> parent;
		/** should the filter publisher be cancelled once we received the first value? */
		final boolean                     cancelOnNext;

		boolean done;

		volatile Subscription sub;

		static final AtomicReferenceFieldUpdater<FilterWhenInner, Subscription> SUB =
				AtomicReferenceFieldUpdater.newUpdater(FilterWhenInner.class, Subscription.class, "sub");

		FilterWhenInner(MonoFilterWhenSubscriber<?> parent, boolean cancelOnNext) {
			this.parent = parent;
			this.cancelOnNext = cancelOnNext;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(SUB, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(Boolean t) {
			if (!done) {
				if (cancelOnNext) {
					sub.cancel();
				}
				done = true;
				parent.innerResult(t);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (!done) {
				done = true;
				parent.innerError(t);
			} else {
				Operators.onErrorDropped(t);
			}
		}

		@Override
		public void onComplete() {
			if (!done) {
				done = true;
				parent.innerComplete();
			}
		}

		void cancel() {
			Operators.terminate(SUB, this);
		}

		@Override
		public Object scan(Attr key) {
			switch(key) {
				case PARENT:
					return parent;
				case ACTUAL:
					return sub;
				case CANCELLED:
					return sub == Operators.cancelledSubscription();
				case TERMINATED:
					return done;
				case PREFETCH:
					return Integer.MAX_VALUE;
				case REQUESTED_FROM_DOWNSTREAM:
					return done ? 0 : 1;
				default:
					return null;
			}
		}
	}
}
