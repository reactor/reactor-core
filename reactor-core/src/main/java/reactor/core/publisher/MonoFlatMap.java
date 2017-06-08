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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.context.Context;

/**
 * Given a Mono source, applies a function on its single item and continues
 * with that Mono instance, emitting its final result.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoFlatMap<T, R> extends MonoOperator<T, R> implements Fuseable {

	final Function<? super T, ? extends Mono<? extends R>> mapper;

	MonoFlatMap(Mono<? extends T> source,
			Function<? super T, ? extends Mono<? extends R>> mapper) {
		super(source);
		this.mapper = Objects.requireNonNull(mapper, "mapper");
	}

	@Override
	public void subscribe(Subscriber<? super R> s, Context ctx) {

		if (FluxFlatMap.trySubscribeScalarMap(source, s, mapper, true)) {
			return;
		}

		FlatMapMain<T, R> manager = new FlatMapMain<>(s, mapper, ctx);
		s.onSubscribe(manager);

		source.subscribe(manager, ctx);
	}

	static final class FlatMapMain<T, R> extends Operators.MonoSubscriber<T, R> {

		final Function<? super T, ? extends Mono<? extends R>> mapper;

		final FlatMapInner<R> second;

		final Context ctx;

		boolean done;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<FlatMapMain, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(FlatMapMain.class,
						Subscription.class,
						"s");

		FlatMapMain(Subscriber<? super R> subscriber,
				Function<? super T, ? extends Mono<? extends R>> mapper,
				Context ctx) {
			super(subscriber);
			this.mapper = mapper;
			this.ctx = ctx;
			this.second = new FlatMapInner<>(this);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(second);
		}

		@Override
		public Context currentContext() {
			return ctx;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == BooleanAttr.TERMINATED) return done;

			return super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}
			done = true;

			Mono<? extends R> m;

			try {
				m = Objects.requireNonNull(mapper.apply(t),
						"The mapper returned a null Mono");
			}
			catch (Throwable ex) {
				actual.onError(Operators.onOperatorError(s, ex, t));
				return;
			}

			if (m instanceof Callable) {
				@SuppressWarnings("unchecked") Callable<R> c = (Callable<R>) m;

				R v;
				try {
					v = c.call();
				}
				catch (Throwable ex) {
					actual.onError(Operators.onOperatorError(s, ex, t));
					return;
				}

				if (v == null) {
					actual.onComplete();
				}
				else {
					complete(v);
				}
				return;
			}

			try {
				m.subscribe(second);
			}
			catch (Throwable e) {
				actual.onError(Operators.onOperatorError(this, e, t));
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
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
		public void cancel() {
			super.cancel();
			Operators.terminate(S, this);
			second.cancel();
		}

		void secondError(Throwable ex) {
			actual.onError(ex);
		}

		void secondComplete() {
			actual.onComplete();
		}
	}

	static final class FlatMapInner<R> implements InnerConsumer<R> {

		final FlatMapMain<?, R> parent;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<FlatMapInner, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(FlatMapInner.class,
						Subscription.class,
						"s");

		boolean done;

		FlatMapInner(FlatMapMain<?, R> parent) {
			this.parent = parent;
		}


		@Override
		public Context currentContext() {
			return parent.ctx;
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == ScannableAttr.ACTUAL) return parent;
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == BooleanAttr.CANCELLED) return s == Operators.cancelledSubscription();

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(R t) {
			if (done) {
				Operators.onNextDropped(t);
				return;
			}
			done = true;
			this.parent.complete(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
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

		void cancel() {
			Operators.terminate(S, this);
		}

	}
}
