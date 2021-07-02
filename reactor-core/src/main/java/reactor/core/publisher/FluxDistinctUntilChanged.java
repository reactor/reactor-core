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
import java.util.function.BiPredicate;
import java.util.function.Function;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Filters out subsequent and repeated elements.
 *
 * @param <T> the value type
 * @param <K> the key type used for comparing subsequent elements
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxDistinctUntilChanged<T, K> extends InternalFluxOperator<T, T> {

	final Function<? super T, K>            keyExtractor;
	final BiPredicate<? super K, ? super K> keyComparator;

	FluxDistinctUntilChanged(Flux<? extends T> source,
			Function<? super T, K> keyExtractor,
			BiPredicate<? super K, ? super K> keyComparator) {
		super(source);
		this.keyExtractor = Objects.requireNonNull(keyExtractor, "keyExtractor");
		this.keyComparator = Objects.requireNonNull(keyComparator, "keyComparator");
	}

	@Override
	@SuppressWarnings("unchecked")
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		if (actual instanceof ConditionalSubscriber) {
			return new DistinctUntilChangedConditionalSubscriber<>((ConditionalSubscriber<? super T>) actual,
					keyExtractor, keyComparator);
		}
		else {
			return new DistinctUntilChangedSubscriber<>(actual, keyExtractor, keyComparator);
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class DistinctUntilChangedSubscriber<T, K>
			implements ConditionalSubscriber<T>, InnerOperator<T, T> {
		final CoreSubscriber<? super T> actual;
		final Context                   ctx;

		final Function<? super T, K> keyExtractor;
		final BiPredicate<? super K, ? super K> keyComparator;

		Subscription s;

		boolean done;

		@Nullable
		K lastKey;

		DistinctUntilChangedSubscriber(CoreSubscriber<? super T> actual,
				Function<? super T, K> keyExtractor,
				BiPredicate<? super K, ? super K> keyComparator) {
			this.actual = actual;
			this.ctx = actual.currentContext();
			this.keyExtractor = keyExtractor;
			this.keyComparator = keyComparator;
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
			if (!tryOnNext(t)) {
				s.request(1);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, ctx);
				return true;
			}

			K k;

			try {
				k = Objects.requireNonNull(keyExtractor.apply(t),
				"The distinct extractor returned a null value.");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, ctx));
				Operators.onDiscard(t, ctx);
				return true;
			}

			if (null == lastKey) {
				lastKey = k;
				actual.onNext(t);
				return true;
			}

			boolean equiv;

			try {
				equiv = keyComparator.test(lastKey, k);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, ctx));
				Operators.onDiscard(t, ctx);
				return true;
			}

			if (equiv) {
				Operators.onDiscard(t, ctx);
				return false;
			}

			lastKey = k;
			actual.onNext(t);
			return true;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, ctx);
				return;
			}
			done = true;
			lastKey = null;

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			lastKey = null;

			actual.onComplete();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
			lastKey = null;
		}
	}

	static final class DistinctUntilChangedConditionalSubscriber<T, K>
			implements ConditionalSubscriber<T>, InnerOperator<T, T> {
		final ConditionalSubscriber<? super T> actual;
		final Context ctx;

		final Function<? super T, K> keyExtractor;
		final BiPredicate<? super K, ? super K> keyComparator;

		Subscription s;

		boolean done;

		@Nullable
		K lastKey;

		DistinctUntilChangedConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				Function<? super T, K> keyExtractor,
				BiPredicate<? super K, ? super K> keyComparator) {
			this.actual = actual;
			this.ctx = actual.currentContext();
			this.keyExtractor = keyExtractor;
			this.keyComparator = keyComparator;
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
			if (!tryOnNext(t)) {
				s.request(1);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, ctx);
				return true;
			}

			K k;

			try {
				k = Objects.requireNonNull(keyExtractor.apply(t),
				"The distinct extractor returned a null value.");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, ctx));
				Operators.onDiscard(t, ctx);
				return true;
			}

			if (null == lastKey) {
				lastKey = k;
				return actual.tryOnNext(t);
			}

			boolean equiv;

			try {
				equiv = keyComparator.test(lastKey, k);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t, ctx));
				Operators.onDiscard(t, ctx);
				return true;
			}

			if (equiv) {
				Operators.onDiscard(t, ctx);
				return false;
			}

			lastKey = k;
			return actual.tryOnNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, ctx);
				return;
			}
			done = true;
			lastKey = null;

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			lastKey = null;

			actual.onComplete();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
			lastKey = null;
		}
	}

}
