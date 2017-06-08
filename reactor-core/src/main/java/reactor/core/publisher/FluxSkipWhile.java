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
import java.util.function.Predicate;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 * Skips source values while a predicate returns
 * true for the value.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSkipWhile<T> extends FluxOperator<T, T> {

	final Predicate<? super T> predicate;

	FluxSkipWhile(Flux<? extends T> source, Predicate<? super T> predicate) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		source.subscribe(new SkipWhileSubscriber<>(s, predicate), ctx);
	}

	static final class SkipWhileSubscriber<T>
			implements ConditionalSubscriber<T>, InnerOperator<T, T> {
		final Subscriber<? super T> actual;

		final Predicate<? super T> predicate;

		Subscription s;

		boolean done;

		boolean skipped;

		SkipWhileSubscriber(Subscriber<? super T> actual, Predicate<? super T> predicate) {
			this.actual = actual;
			this.predicate = predicate;
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
				Operators.onNextDropped(t);
				return;
			}

			if (skipped){
				actual.onNext(t);
				return;
			}
			boolean b;

			try {
				b = predicate.test(t);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return;
			}

			if (b) {
				s.request(1);
				return;
			}

			skipped = true;
			actual.onNext(t);
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t);
				return true;
			}

			if (skipped) {
				actual.onNext(t);
				return true;
			}
			boolean b;

			try {
				b = predicate.test(t);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));

				return true;
			}

			if (b) {
				return false;
			}

			skipped = true;
			actual.onNext(t);
			return true;
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
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Subscriber<? super T> actual() {
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
