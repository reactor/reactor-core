/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Loopback;

/**
 * Resumes the failed main sequence with another sequence returned by
 * a function for the particular failure exception.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxResume<T> extends FluxSource<T, T> {

	final Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory;

	public FluxResume(Publisher<? extends T> source,
			Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory) {
		super(source);
		this.nextFactory = Objects.requireNonNull(nextFactory, "nextFactory");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new ResumeSubscriber<>(s, nextFactory));
	}

	static final class ResumeSubscriber<T>
			extends Operators.MultiSubscriptionSubscriber<T, T> implements Loopback {

		final Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory;

		boolean second;

		public ResumeSubscriber(Subscriber<? super T> actual,
				Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory) {
			super(actual);
			this.nextFactory = nextFactory;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (!second) {
				subscriber.onSubscribe(this);
			}
			set(s);
		}

		@Override
		public void onNext(T t) {
			subscriber.onNext(t);

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
					Throwable _e = Operators.onOperatorError(e);
					if (t != _e) {
						_e.addSuppressed(t);
					}
					subscriber.onError(_e);
					return;
				}
				p.subscribe(this);
			}
			else {
				subscriber.onError(t);
			}
		}

		@Override
		public Object connectedInput() {
			return nextFactory;
		}


	}
}
