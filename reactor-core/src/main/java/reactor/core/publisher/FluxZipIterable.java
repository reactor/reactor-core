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

import java.util.Iterator;
import java.util.Objects;
import java.util.function.BiFunction;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * Pairwise combines elements of a publisher and an iterable sequence through a function.
 *
 * @param <T> the main source value type
 * @param <U> the iterable source value type
 * @param <R> the result type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxZipIterable<T, U, R> extends InternalFluxOperator<T, R> {

	final Iterable<? extends U> other;

	final BiFunction<? super T, ? super U, ? extends R> zipper;

	FluxZipIterable(Flux<? extends T> source,
			Iterable<? extends U> other,
			BiFunction<? super T, ? super U, ? extends R> zipper) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
		this.zipper = Objects.requireNonNull(zipper, "zipper");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		Iterator<? extends U> it;

		try {
			it = Objects.requireNonNull(other.iterator(),
					"The other iterable produced a null iterator");
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return null;
		}

		boolean b;

		try {
			b = it.hasNext();
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return null;
		}

		if (!b) {
			Operators.complete(actual);
			return null;
		}

		return new ZipSubscriber<>(actual, it, zipper);
	}

	static final class ZipSubscriber<T, U, R>
			implements InnerOperator<T, R> {

		final CoreSubscriber<? super R> actual;

		final Iterator<? extends U> it;

		final BiFunction<? super T, ? super U, ? extends R> zipper;

		Subscription s;

		boolean done;

		ZipSubscriber(CoreSubscriber<? super R> actual,
				Iterator<? extends U> it,
				BiFunction<? super T, ? super U, ? extends R> zipper) {
			this.actual = actual;
			this.it = it;
			this.zipper = zipper;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PARENT) return s;

			return InnerOperator.super.scanUnsafe(key);
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

			U u;

			try {
				u = it.next();
			}
			catch (Throwable e) {
				done = true;
				actual.onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				return;
			}

			R r;

			try {
				r = Objects.requireNonNull(zipper.apply(t, u),
						"The zipper returned a null value");
			}
			catch (Throwable e) {
				done = true;
				actual.onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				return;
			}

			actual.onNext(r);

			boolean b;

			try {
				b = it.hasNext();
			}
			catch (Throwable e) {
				done = true;
				actual.onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				return;
			}

			if (!b) {
				done = true;
				s.cancel();
				actual.onComplete();
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
		public CoreSubscriber<? super R> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}
	}
}
