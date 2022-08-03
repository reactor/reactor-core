/*
 * Copyright (c) 2017-2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static reactor.core.Scannable.Attr.RUN_STYLE;
import static reactor.core.Scannable.Attr.RunStyle.SYNC;

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
class MonoFilterWhen<T> extends InternalMonoOperator<T, T> {

	final Function<? super T, ? extends Publisher<Boolean>> asyncPredicate;

	MonoFilterWhen(Mono<T> source,
			Function<? super T, ? extends Publisher<Boolean>> asyncPredicate) {
		super(source);
		this.asyncPredicate = asyncPredicate;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new MonoFilterWhenMain<>(actual, asyncPredicate);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == RUN_STYLE) return SYNC;
		return super.scanUnsafe(key);
	}

	static final class MonoFilterWhenMain<T> implements InnerOperator<T, T>,
	                                                    Fuseable, //for constants only
	                                                    Fuseable.QueueSubscription<T> {

		final Function<? super T, ? extends Publisher<Boolean>> asyncPredicate;
		final CoreSubscriber<? super T> actual;

		Subscription s;

		boolean done;

		volatile FilterWhenInner<T> asyncFilter;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MonoFilterWhenMain, FilterWhenInner> ASYNC_FILTER =
				AtomicReferenceFieldUpdater.newUpdater(MonoFilterWhenMain.class, FilterWhenInner.class, "asyncFilter");

		@SuppressWarnings({"ConstantConditions", "rawtypes"})
		static final FilterWhenInner INNER_CANCELLED = new FilterWhenInner(null, false, null);
		@SuppressWarnings({"ConstantConditions", "rawtypes"})
		static final FilterWhenInner INNER_TERMINATED = new FilterWhenInner(null, false, null);

		MonoFilterWhenMain(CoreSubscriber<? super T> actual, Function<? super T, ?
				extends Publisher<Boolean>> asyncPredicate) {
			this.actual = actual;
			this.asyncPredicate = asyncPredicate;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				this.actual.onSubscribe(this);
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onNext(T t) {
			this.done = true;
			//we assume the source is a Mono, so only one onNext will ever happen
			Publisher<Boolean> p;

			try {
				p = Objects.requireNonNull(asyncPredicate.apply(t),
						"The asyncPredicate returned a null value");
			}
			catch (Throwable ex) {
				Exceptions.throwIfFatal(ex);
				Operators.onDiscard(t, actual.currentContext());
				this.actual.onError(ex);
				return;
			}

			if (p instanceof Callable) {
				Boolean u;

				try {
					u = ((Callable<Boolean>) p).call();
				}
				catch (Throwable ex) {
					Exceptions.throwIfFatal(ex);
					Operators.onDiscard(t, actual.currentContext());
					this.actual.onError(ex);
					return;
				}

				if (u != null && u) {
					this.actual.onNext(t);
					this.actual.onComplete();
				}
				else {
					Operators.onDiscard(t, actual.currentContext());
					actual.onComplete();
				}
			}
			else {
				FilterWhenInner<T> inner = new FilterWhenInner<>(this, !(p instanceof Mono), t);
				p.subscribe(inner);
			}
		}

		@Override
		public void onComplete() {
			if (this.done) {
				return;
			}

			//there was no value, we can complete empty
			this.done = true;
			this.actual.onComplete();
		}

		@Override
		public void onError(Throwable t) {
			if (this.done) {
				Operators.onErrorDropped(t, currentContext());
				return;
			}

			//there was no value, we can complete empty
			this.done = true;

			/* implementation note on onError:
			 * if the source errored, we can propagate that directly since there
			 * was no chance for an inner subscriber to have been triggered
			 * (the source being a Mono). So we can just have the parent's behavior
			 * of calling actual.onError(t) for onError.
			 */
			this.actual.onError(t);
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			this.s.cancel();

			final FilterWhenInner<T> a = asyncFilter;
			if (a != INNER_CANCELLED && a != INNER_TERMINATED && ASYNC_FILTER.compareAndSet(this, a, INNER_CANCELLED)) {
				if (a != null) {
					a.cancel();
				}
			}
		}

		public boolean trySetInner(FilterWhenInner<T> inner) {
			final FilterWhenInner<T> a = this.asyncFilter;
			if (a == null && ASYNC_FILTER.compareAndSet(this, null, inner)) {
				return true;
			}
			Operators.onDiscard(inner.value, currentContext());
			return false;
		}

		void innerResult(boolean item, FilterWhenInner<T> inner) {
			final FilterWhenInner<T> a = this.asyncFilter;
			if (a == inner && ASYNC_FILTER.compareAndSet(this, inner, INNER_TERMINATED)) {
				if (item) {
					//will reset the value with itself, but using parent's `value` saves a field
					this.actual.onNext(inner.value);
					this.actual.onComplete();
				}
				else {
					Operators.onDiscard(inner.value, currentContext());
					this.actual.onComplete();
				}
			}
			// do nothing, value already discarded
		}

		void innerError(Throwable ex, FilterWhenInner<T> inner) {
			//if the inner subscriber (the filter one) errors, then we can
			//always propagate that error directly, as it means that the source Mono
			//was at least valued rather than in error.
			final FilterWhenInner<T> a = this.asyncFilter;
			if (a == inner && ASYNC_FILTER.compareAndSet(this, inner, INNER_TERMINATED)) {
				Operators.onDiscard(inner.value, currentContext());
				this.actual.onError(ex);
			}

			Operators.onErrorDropped(ex, currentContext());

			// do nothing with value, value already discarded
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.PREFETCH) return 0;
			if (key == Attr.TERMINATED) {
				final FilterWhenInner<T> af = asyncFilter;
				return done && (af == null || af == INNER_TERMINATED);
			}
			if (key == Attr.CANCELLED) return asyncFilter == INNER_CANCELLED;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			//CANCELLED, PREFETCH
			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			final FilterWhenInner<T> c = asyncFilter;
			return c == null || c == INNER_CANCELLED || c == INNER_TERMINATED ? Stream.empty() : Stream.of(c);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		@Override
		public T poll() {
			return null;
		}

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public boolean isEmpty() {
			return true;
		}

		@Override
		public void clear() {

		}
	}

	static final class FilterWhenInner<T> implements InnerConsumer<Boolean> {

		final MonoFilterWhenMain<T> parent;
		/** should the filter publisher be cancelled once we received the first value? */
		final boolean               cancelOnNext;

		final T value;

		boolean done;

	    Subscription s;

		FilterWhenInner(MonoFilterWhenMain<T> parent, boolean cancelOnNext, T value) {
			this.parent = parent;
			this.cancelOnNext = cancelOnNext;
			this.value = value;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				if (this.parent.trySetInner(this)) {
					s.request(Long.MAX_VALUE);
				} else {
					s.cancel();
				}
			}
		}

		@Override
		public void onNext(Boolean t) {
			if (done) {
				return;
			}

			done = true;

			if (cancelOnNext) {
				s.cancel();
			}

			parent.innerResult(t, this);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, parent.currentContext());
				return;
			}

			done = true;
			parent.innerError(t, this);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}

			//the filter publisher was empty
			done = true;
			parent.innerResult(false, this); //will trigger actual.onComplete()
		}

		@Override
		public Context currentContext() {
			return parent.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.ACTUAL) return parent;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PREFETCH) return 0;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return done ? 0L : 1L;
			if (key == Attr.RUN_STYLE) return SYNC;

			return null;
		}

		void cancel() {
			this.s.cancel();
		}
	}
}
