/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.function.BiFunction;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple2;

/**
 * A {@link reactor.core.Fuseable} version of {@link FluxIndex}, an
 * operator that tags the values it passes through with their index in the original
 * sequence, either as their natural long index (0-based) or as a customized index
 * by way of a user-provided {@link BiFunction}. The resulting sequence is one of
 * {@link Tuple2}, t1 value being the index (as mapped by the user function) and t2 the
 * source value.
 *
 * @author Simon Baslé
 */
final class FluxIndexFuseable<T, I> extends InternalFluxOperator<T, I>
		implements Fuseable {

	private final BiFunction<? super Long, ? super T, ? extends I> indexMapper;

	FluxIndexFuseable(Flux<T> source,
			BiFunction<? super Long, ? super T, ? extends I> indexMapper) {
		super(source);
		this.indexMapper = FluxIndex.NullSafeIndexMapper.create(Objects.requireNonNull(indexMapper,
				"indexMapper must be non null"));
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super I> actual) {
		if (actual instanceof ConditionalSubscriber) {
			@SuppressWarnings("unchecked") ConditionalSubscriber<? super I> cs =
					(ConditionalSubscriber<? super I>) actual;
			return new IndexFuseableConditionalSubscriber<>(cs, indexMapper);
		}
		else {
			return new IndexFuseableSubscriber<>(actual, indexMapper);
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class IndexFuseableSubscriber<I, T> implements InnerOperator<T, I>,
	                                                            QueueSubscription<I> {

		final CoreSubscriber<? super I>             actual;
		final BiFunction<? super Long, ? super T, ? extends I> indexMapper;

		boolean              done;
		long                 index;
		QueueSubscription<T> s;
		int                  sourceMode;

		IndexFuseableSubscriber(CoreSubscriber<? super I> actual,
				BiFunction<? super Long, ? super T, ? extends I> indexMapper) {
			this.actual = actual;
			this.indexMapper = indexMapper;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				@SuppressWarnings("unchecked")
				QueueSubscription<T> qs = (QueueSubscription<T>) s;
				this.s = qs;

				actual.onSubscribe(this);
			}
		}

		@Override
		@Nullable
		public I poll() {
			T v = s.poll();
			if (v != null) {
				long i = this.index;
				I indexed = indexMapper.apply(i, v);
				this.index = i + 1;
				return indexed;
			}
			return null;
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

				long i = this.index;
				try {
					I indexed = indexMapper.apply(i, t);
					this.index = i + 1L;
					actual.onNext(indexed);
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				}
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
		public CoreSubscriber<? super I> actual() {
			return this.actual;
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
			if (indexMapper != Flux.TUPLE2_BIFUNCTION && (requestedMode & Fuseable.THREAD_BARRIER) != 0) {
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

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}
	}

	static final class IndexFuseableConditionalSubscriber<I, T>
			implements InnerOperator<T, I>,
			           ConditionalSubscriber<T>,
			           QueueSubscription<I> {

		final ConditionalSubscriber<? super I>      actual;
		final BiFunction<? super Long, ? super T, ? extends I> indexMapper;

		boolean              done;
		long                 index;
		QueueSubscription<T> s;
		int                  sourceMode;

		IndexFuseableConditionalSubscriber(
				ConditionalSubscriber<? super I> cs,
				BiFunction<? super Long, ? super T, ? extends I> indexMapper) {
			this.actual = cs;
			this.indexMapper = indexMapper;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				@SuppressWarnings("unchecked")
				QueueSubscription<T> qs = (QueueSubscription<T>) s;
				this.s = qs;
				actual.onSubscribe(this);
			}
		}

		@Override
		@Nullable
		public I poll() {
			T v = s.poll();
			if (v != null) {
				long i = this.index;
				I indexed = indexMapper.apply(i, v);
				this.index = i + 1;
				return indexed;
			}
			return null;
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return true;
			}

			I indexed;
			long i = this.index;
			try {
				indexed = indexMapper.apply(i, t);
				this.index = i + 1L;
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				return true;
			}

			return actual.tryOnNext(indexed);
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

				long i = this.index;
				try {
					I indexed = indexMapper.apply(i, t);
					this.index = i + 1L;
					actual.onNext(indexed);
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				}
			}
		}

		@Override
		public void onError(Throwable throwable) {
			if (done) {
				Operators.onErrorDropped(throwable, actual.currentContext());
				return;
			}

			done = true;

			actual.onError(throwable);
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
		public CoreSubscriber<? super I> actual() {
			return this.actual;
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
			if (indexMapper != Flux.TUPLE2_BIFUNCTION && (requestedMode & Fuseable.THREAD_BARRIER) != 0) {
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

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}
	}
}
