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
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;

/**
 * Resumes the failed main sequence with another sequence returned by
 * a function for the particular failure exception.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxOnErrorResume<T> extends InternalFluxOperator<T, T> {

	final Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory;

	FluxOnErrorResume(Flux<? extends T> source,
			Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory) {
		super(source);
		this.nextFactory = Objects.requireNonNull(nextFactory, "nextFactory");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new ResumeSubscriber<>(actual, nextFactory);
	}

	static final class ResumeSubscriber<T>
			extends Operators.MultiSubscriptionSubscriber<T, T> {

		final Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory;

		boolean second;

		ResumeSubscriber(CoreSubscriber<? super T> actual,
				Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory) {
			super(actual);
			this.nextFactory = nextFactory;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (!second) {
				actual.onSubscribe(this);
			}
			set(s);
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);

			if (!second) {
				producedOne();
			}
		}

		@Override
		public void onError(Throwable t) {
			if (!second) {
				second = true;

				Publisher<? extends T> p;

				try {
					p = Objects.requireNonNull(nextFactory.apply(t),
					"The nextFactory returned a null Publisher");
				}
				catch (Throwable e) {
					Throwable _e = Operators.onOperatorError(e, actual.currentContext());
					_e = Exceptions.addSuppressed(_e, t);
					actual.onError(_e);
					return;
				}
				p.subscribe(this);
			}
			else {
				actual.onError(t);
			}
		}

	}
}
