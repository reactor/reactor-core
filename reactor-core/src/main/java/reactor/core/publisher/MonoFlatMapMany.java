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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import javax.annotation.Nullable;
import reactor.util.context.Context;

final class MonoFlatMapMany<T, R> extends FluxOperator<T, R> {


	final Function<? super T, ? extends Publisher<? extends R>> mapper;

	MonoFlatMapMany(Mono<? extends T> source,
			Function<? super T, ? extends Publisher<? extends R>> mapper) {
		super(source);
		this.mapper = mapper;
	}

	@Override
	public void subscribe(Subscriber<? super R> s, Context ctx) {
		if (FluxFlatMap.trySubscribeScalarMap(source, s, mapper, false)) {
			return;
		}
		source.subscribe(new FlatMapManyMain<T, R>(s, mapper, ctx), ctx);
	}

	static final class FlatMapManyMain<T, R> implements InnerOperator<T, R> {

		final Subscriber<? super R> actual;

		final Context ctx;

		final Function<? super T, ? extends Publisher<? extends R>> mapper;

		Subscription main;

		volatile Subscription inner;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<FlatMapManyMain, Subscription> INNER =
				AtomicReferenceFieldUpdater.newUpdater(FlatMapManyMain.class,
						Subscription.class,
						"inner");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<FlatMapManyMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(FlatMapManyMain.class, "requested");

		boolean hasValue;

		FlatMapManyMain(Subscriber<? super R> actual,
				Function<? super T, ? extends Publisher<? extends R>> mapper,
				Context ctx) {
			this.ctx = ctx;
			this.actual = actual;
			this.mapper = mapper;
		}

		@Override
		public Context currentContext() {
			return ctx;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return main;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(Scannable.from(inner));
		}

		@Override
		public Subscriber<? super R> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			Subscription a = inner;
			if (a != null) {
				a.request(n);
			}
			else {
				if (Operators.validate(n)) {
					Operators.getAndAddCap(REQUESTED, this, n);
					a = inner;
					if (a != null) {
						n = REQUESTED.getAndSet(this, 0L);
						if (n != 0L) {
							a.request(n);
						}
					}
				}
			}
		}

		@Override
		public void cancel() {
			main.cancel();
			Operators.terminate(INNER, this);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.main, s)) {
				this.main = s;

				actual.onSubscribe(this);

				s.request(Long.MAX_VALUE);
			}
		}

		void onSubscribeInner(Subscription s) {
			if (Operators.setOnce(INNER, this, s)) {

				long r = REQUESTED.getAndSet(this, 0L);
				if (r != 0) {
					s.request(r);
				}
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onNext(T t) {
			hasValue = true;

			Publisher<? extends R> p;

			try {
				p = Objects.requireNonNull(mapper.apply(t),
						"The mapper returned a null Publisher.");
			}
			catch (Throwable ex) {
				actual.onError(Operators.onOperatorError(this, ex, t));
				return;
			}

			if (p instanceof Callable) {
				R v;

				try {
					v = ((Callable<R>) p).call();
				}
				catch (Throwable ex) {
					actual.onError(Operators.onOperatorError(this, ex, t));
					return;
				}

				if (v == null) {
					actual.onComplete();
				}
				else {
					onSubscribeInner(Operators.scalarSubscription(actual, v));
				}

				return;
			}

			p.subscribe(new FlatMapManyInner<>(this, actual));
		}

		@Override
		public void onError(Throwable t) {
			if (hasValue) {
				Operators.onErrorDropped(t);
				return;
			}
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (!hasValue) {
				actual.onComplete();
			}
		}
	}

	static final class FlatMapManyInner<R> implements InnerConsumer<R> {

		final FlatMapManyMain<?, R> parent;

		final Subscriber<? super R> actual;

		FlatMapManyInner(FlatMapManyMain<?, R> parent,
				Subscriber<? super R> actual) {
			this.parent = parent;
			this.actual = actual;
		}

		@Override
		public Context currentContext() {
			return parent.ctx;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return parent.inner;
			if (key == ScannableAttr.ACTUAL) return parent;
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return parent.requested;

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			parent.onSubscribeInner(s);
		}

		@Override
		public void onNext(R t) {
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

	}
}
