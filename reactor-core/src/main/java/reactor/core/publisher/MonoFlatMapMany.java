/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class MonoFlatMapMany<T, R> extends FluxFromMonoOperator<T, R> {


	final Function<? super T, ? extends Publisher<? extends R>> mapper;

	MonoFlatMapMany(Mono<? extends T> source,
			Function<? super T, ? extends Publisher<? extends R>> mapper) {
		super(source);
		this.mapper = mapper;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		//for now Mono in general doesn't support onErrorContinue, so the scalar version shouldn't either
		//even if the result is a Flux. once the mapper is applied, onErrorContinue will be taken care of by
		//the mapped Flux if relevant.
		if (FluxFlatMap.trySubscribeScalarMap(source, actual, mapper, false, false)) {
			return null;
		}
		return new FlatMapManyMain<T, R>(actual, mapper);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class FlatMapManyMain<T, R> implements InnerOperator<T, R> {

		final CoreSubscriber<? super R> actual;

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

		FlatMapManyMain(CoreSubscriber<? super R> actual,
				Function<? super T, ? extends Publisher<? extends R>> mapper) {
			this.actual = actual;
			this.mapper = mapper;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return main;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(Scannable.from(inner));
		}

		@Override
		public CoreSubscriber<? super R> actual() {
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
					Operators.addCap(REQUESTED, this, n);
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
				//if the mapping fails, then there is nothing to be continued, since the source is a Mono
				actual.onError(Operators.onOperatorError(this, ex, t,
						actual.currentContext()));
				return;
			}

			if (p instanceof Callable) {
				R v;

				try {
					v = ((Callable<R>) p).call();
				}
				catch (Throwable ex) {
					actual.onError(Operators.onOperatorError(this, ex, t,
							actual.currentContext()));
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
				Operators.onErrorDropped(t, actual.currentContext());
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

		final CoreSubscriber<? super R> actual;

		FlatMapManyInner(FlatMapManyMain<?, R> parent,
				CoreSubscriber<? super R> actual) {
			this.parent = parent;
			this.actual = actual;
		}

		@Override
		public Context currentContext() {
			return actual.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return parent.inner;
			if (key == Attr.ACTUAL) return parent;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return parent.requested;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

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
