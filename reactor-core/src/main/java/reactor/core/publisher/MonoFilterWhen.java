/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

	static final class MonoFilterWhenMain<T> extends Operators.MonoSubscriber<T, T> {

		/* Implementation notes on state transitions:
		 * This subscriber runs through a few possible state transitions, that are
		 * expressed through the signal methods rather than an explicit state variable,
		 * as they are simple enough (states suffixed with a * correspond to a terminal
		 * signal downstream):
		 *  - SUBSCRIPTION -> EMPTY | VALUED | EARLY ERROR
		 *  - EMPTY -> COMPLETE
		 *  - VALUED -> FILTERING | EARLY ERROR
		 *  - EARLY ERROR*
		 *  - FILTERING -> FEMPTY | FERROR | FVALUED
		 *  - FEMPTY -> COMPLETE
		 *  - FERROR*
		 *  - FVALUED -> ON NEXT + COMPLETE | COMPLETE
		 *  - COMPLETE*
		 */

		final Function<? super T, ? extends Publisher<Boolean>> asyncPredicate;

		//this is only touched by onNext and read by onComplete, so no need for volatile
		boolean sourceValued;

		Subscription upstream;

		volatile FilterWhenInner asyncFilter;

		static final AtomicReferenceFieldUpdater<MonoFilterWhenMain, FilterWhenInner> ASYNC_FILTER =
				AtomicReferenceFieldUpdater.newUpdater(MonoFilterWhenMain.class, FilterWhenInner.class, "asyncFilter");

		@SuppressWarnings("ConstantConditions")
		static final FilterWhenInner INNER_CANCELLED = new FilterWhenInner(null, false);

		MonoFilterWhenMain(CoreSubscriber<? super T> actual, Function<? super T, ?
				extends Publisher<Boolean>> asyncPredicate) {
			super(actual);
			this.asyncPredicate = asyncPredicate;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(upstream, s)) {
				upstream = s;
				actual.onSubscribe(this);
				s.request(Long.MAX_VALUE);
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onNext(T t) {
			//we assume the source is a Mono, so only one onNext will ever happen
			sourceValued = true;
			setValue(t);
			Publisher<Boolean> p;

			try {
				p = Objects.requireNonNull(asyncPredicate.apply(t),
						"The asyncPredicate returned a null value");
			}
			catch (Throwable ex) {
				Exceptions.throwIfFatal(ex);
				super.onError(ex);
				Operators.onDiscard(t, actual.currentContext());
				return;
			}

			if (p instanceof Callable) {
				Boolean u;

				try {
					u = ((Callable<Boolean>) p).call();
				}
				catch (Throwable ex) {
					Exceptions.throwIfFatal(ex);
					super.onError(ex);
					Operators.onDiscard(t, actual.currentContext());
					return;
				}

				if (u != null && u) {
					complete(t);
				}
				else {
					actual.onComplete();
					Operators.onDiscard(t, actual.currentContext());
				}
			}
			else {
				FilterWhenInner inner = new FilterWhenInner(this, !(p instanceof Mono));
				if (ASYNC_FILTER.compareAndSet(this, null, inner)) {
					p.subscribe(inner);
				}
			}
		}

		@Override
		public void onComplete() {
			if (!sourceValued) {
				//there was no value, we can complete empty
				super.onComplete();
			}
			//otherwise just wait for the inner filter to apply, rather than complete too soon
		}

		/* implementation note on onError:
		 * if the source errored, we can propagate that directly since there
		 * was no chance for an inner subscriber to have been triggered
		 * (the source being a Mono). So we can just have the parent's behavior
		 * of calling actual.onError(t) for onError.
		 */

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

		void innerResult(@Nullable Boolean item) {
			if (item != null && item) {
				//will reset the value with itself, but using parent's `value` saves a field
				complete(this.value);
			}
			else {
				super.onComplete();
				discard(this.value);
			}
		}

		void innerError(Throwable ex) {
			//if the inner subscriber (the filter one) errors, then we can
			//always propagate that error directly, as it means that the source Mono
			//was at least valued rather than in error.
			super.onError(ex);
			discard(this.value);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return upstream;
			if (key == Attr.TERMINATED) return asyncFilter != null
					? asyncFilter.scanUnsafe(Attr.TERMINATED)
					: super.scanUnsafe(Attr.TERMINATED);
			if (key == RUN_STYLE) return Attr.RunStyle.SYNC;

			//CANCELLED, PREFETCH
			return super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			FilterWhenInner c = asyncFilter;
			return c == null ? Stream.empty() : Stream.of(c);
		}
	}

	static final class FilterWhenInner implements InnerConsumer<Boolean> {

		final MonoFilterWhenMain<?> main;
		/** should the filter publisher be cancelled once we received the first value? */
		final boolean               cancelOnNext;

		boolean done;

		volatile Subscription sub;

		static final AtomicReferenceFieldUpdater<FilterWhenInner, Subscription> SUB =
				AtomicReferenceFieldUpdater.newUpdater(FilterWhenInner.class, Subscription.class, "sub");

		FilterWhenInner(MonoFilterWhenMain<?> main, boolean cancelOnNext) {
			this.main = main;
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
				main.innerResult(t);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (!done) {
				done = true;
				main.innerError(t);
			} else {
				Operators.onErrorDropped(t, main.currentContext());
			}
		}

		@Override
		public Context currentContext() {
			return main.currentContext();
		}

		@Override
		public void onComplete() {
			if (!done) {
				//the filter publisher was empty
				done = true;
				main.innerResult(null); //will trigger actual.onComplete()
			}
		}

		void cancel() {
			Operators.terminate(SUB, this);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return sub;
			if (key == Attr.ACTUAL) return main;
			if (key == Attr.CANCELLED) return sub == Operators.cancelledSubscription();
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return done ? 0L : 1L;
			if (key == RUN_STYLE) return SYNC;

			return null;
		}
	}
}
