/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple2;

/**
 * An operator that tags the values it passes through with their index in the original
 * sequence as their natural long index (0-based) and maps it to a container type
 * by way of a user-provided {@link BiFunction}. Usually the container type would be
 * a {@link Tuple2}, t1 value being the index (as mapped by the user function) and t2 the
 * source value, but this is entirely up to the user function to decide.
 *
 * @author Simon Baslé
 */
final class FluxIndex<T, I> extends InternalFluxOperator<T, I> {

	private final BiFunction<? super Long, ? super T, ? extends I> indexMapper;

	FluxIndex(Flux<T> source,
			BiFunction<? super Long, ? super T, ? extends I> indexMapper) {
		super(source);
		this.indexMapper = NullSafeIndexMapper.create(Objects.requireNonNull(indexMapper,
				"indexMapper must be non null"));
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super I> actual) {
		if (actual instanceof ConditionalSubscriber) {
			@SuppressWarnings("unchecked") ConditionalSubscriber<? super I> cs =
					(ConditionalSubscriber<? super I>) actual;
			return new IndexConditionalSubscriber<>(cs, indexMapper);
		}
		else {
			return new IndexSubscriber<>(actual, indexMapper);
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class IndexSubscriber<T, I> implements InnerOperator<T, I> {

		final CoreSubscriber<? super I>                        actual;
		final BiFunction<? super Long, ? super T, ? extends I> indexMapper;

		boolean      done;
		Subscription s;
		long         index = 0;

		IndexSubscriber(CoreSubscriber<? super I> actual,
				BiFunction<? super Long, ? super T, ? extends I> indexMapper) {
			this.actual = actual;
			this.indexMapper = indexMapper;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			long i = this.index;
			try {
				I typedIndex = indexMapper.apply(i, t);
				this.index = i + 1L;
				actual.onNext(typedIndex);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
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
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}
	}

	static final class IndexConditionalSubscriber<T, I> implements InnerOperator<T, I>,
	                                                               ConditionalSubscriber<T> {

		final ConditionalSubscriber<? super I>      actual;
		final BiFunction<? super Long, ? super T, ? extends I> indexMapper;

		Subscription s;
		boolean done;
		long index;

		IndexConditionalSubscriber(
				ConditionalSubscriber<? super I> cs,
				BiFunction<? super Long, ? super T, ? extends I> indexMapper) {
			this.actual = cs;
			this.indexMapper = indexMapper;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return true;
			}

			I typedIndex;
			long i = this.index;
			try {
				typedIndex = indexMapper.apply(i, t);
				this.index = i + 1L;
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				return true;
			}

			return actual.tryOnNext(typedIndex);
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			long i = this.index;
			try {
				I typedIndex = indexMapper.apply(i, t);
				this.index = i + 1L;
				actual.onNext(typedIndex);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
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
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}
	}

	static class NullSafeIndexMapper<T, I> implements BiFunction<Long, T, I> {

		private final BiFunction<? super Long, ? super T, ? extends I> indexMapper;

		private NullSafeIndexMapper(BiFunction<? super Long, ? super T, ? extends I> indexMapper) {
			this.indexMapper = indexMapper;
		}

		@Override
		public I apply(Long i, T t) {
			I typedIndex = indexMapper.apply(i, t);
			if (typedIndex == null) {
				throw new NullPointerException("indexMapper returned a null value" +
						" at raw index " + i + " for value " + t);
			}
			return typedIndex;
		}

		static <T, I> BiFunction<? super Long, ? super T, ? extends I> create(
				BiFunction<? super Long, ? super T, ? extends I> indexMapper) {
			if (indexMapper == Flux.TUPLE2_BIFUNCTION) {
				// TUPLE2_BIFUNCTION (Tuples::of) never returns null.
				// Also helps FluxIndexFuseable to detect the default index mapper.
				return indexMapper;
			}
			return new NullSafeIndexMapper<>(indexMapper);
		}
	}
}
