/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Given a Mono source, applies a function on its single item and continues
 * with that Mono instance, emitting its final result.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoFlatMap<T, R> extends InternalMonoOperator<T, R> implements Fuseable {

	final Function<? super T, ? extends Mono<? extends R>> mapper;

	MonoFlatMap(Mono<? extends T> source,
			Function<? super T, ? extends Mono<? extends R>> mapper) {
		super(source);
		this.mapper = Objects.requireNonNull(mapper, "mapper");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		//for now Mono in general doesn't support onErrorContinue, so the scalar version shouldn't either
		if (FluxFlatMap.trySubscribeScalarMap(source, actual, mapper, true, false)) {
			return null;
		}

		return new FlatMapMain<>(actual, mapper);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class FlatMapMain<T, R> implements InnerOperator<T, R>,
	                                                Fuseable, //for constants only
			                                        QueueSubscription<R> {

		final Function<? super T, ? extends Mono<? extends R>> mapper;

		final CoreSubscriber<? super R> actual;

		boolean done;

		Subscription s;

		volatile FlatMapInner<R> second;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<FlatMapMain, FlatMapInner> SECOND =
				AtomicReferenceFieldUpdater.newUpdater(FlatMapMain.class, FlatMapInner.class, "second");

		FlatMapMain(CoreSubscriber<? super R> actual,
				Function<? super T, ? extends Mono<? extends R>> mapper) {
			this.actual = actual;
			this.mapper = mapper;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(second);
		}

		@Override
		public CoreSubscriber<? super R> actual() {
			return this.actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.PREFETCH) return 0;
			if (key == Attr.CANCELLED) return second == FlatMapInner.CANCELLED;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				this.actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			done = true;

			Mono<? extends R> m;

			try {
				m = Objects.requireNonNull(mapper.apply(t),
						"The mapper returned a null Mono");
			}
			catch (Throwable ex) {
				actual.onError(Operators.onOperatorError(s, ex, t,
						actual.currentContext()));
				return;
			}

			if (m instanceof Callable) {
				@SuppressWarnings("unchecked") Callable<R> c = (Callable<R>) m;

				R v;
				try {
					v = c.call();
				}
				catch (Throwable ex) {
					actual.onError(Operators.onOperatorError(s, ex, t,
							actual.currentContext()));
					return;
				}

				if (v == null) {
					actual.onComplete();
				}
				else {
					actual.onNext(v);
					actual.onComplete();
				}
				return;
			}

			try {
				m.subscribe(new FlatMapInner<>(this));
			}
			catch (Throwable e) {
				actual.onError(Operators.onOperatorError(this, e, t,
						actual.currentContext()));
			}
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
		public void request(long n) {
			this.s.request(n);
		}

		@Override
		public void cancel() {
			this.s.cancel();

			final FlatMapInner<R> second = this.second;
			if (second == FlatMapInner.CANCELLED || !SECOND.compareAndSet(this, second, FlatMapInner.CANCELLED)) {
				return;
			}

			if (second != null) {
				second.s.cancel();
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE;
		}

		@Override
		public R poll() {
			return null;
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

		boolean setSecond(FlatMapInner<R> inner) {
			return this.second == null && SECOND.compareAndSet(this, null, inner);
		}

		void secondError(Throwable ex) {
			actual.onError(ex);
		}

		void secondComplete(R t) {
			actual.onNext(t);
			actual.onComplete();
		}

		void secondComplete() {
			actual.onComplete();
		}
	}

	static final class FlatMapInner<R> implements InnerConsumer<R> {

		static final FlatMapInner<?> CANCELLED = new FlatMapInner<>(null);

		final FlatMapMain<?, R> parent;

		Subscription s;

		boolean done;

		FlatMapInner(FlatMapMain<?, R> parent) {
			this.parent = parent;
		}

		@Override
		public Context currentContext() {
			return parent.currentContext();
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.ACTUAL) return parent;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				if (this.parent.setSecond(this)) {
					s.request(Long.MAX_VALUE);
				} else {
					s.cancel();
				}
			}
		}

		@Override
		public void onNext(R t) {
			if (done) {
				Operators.onNextDropped(t, parent.currentContext());
				return;
			}
			done = true;
			this.parent.secondComplete(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, parent.currentContext());
				return;
			}
			done = true;
			this.parent.secondError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			this.parent.secondComplete();
		}
	}
}
