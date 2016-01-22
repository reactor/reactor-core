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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.subscriber.SubscriberMultiSubscription;
import reactor.core.trait.Connectable;
import reactor.core.util.Exceptions;
import reactor.fn.Function;

/**
 * Resumes the failed main sequence with another sequence returned by
 * a function for the particular failure exception.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxResume<T> extends Flux.FluxBarrier<T, T> {

	final Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory;

// FIXME this causes ambiguity error because javac can't distinguish between different lambda arities:
//
//	new FluxResume(source, e -> other) tries to match the (Publisher, Publisher) constructor and fails
//
//	public FluxResume(Publisher<? extends T> source,
//			Publisher<? extends T> next) {
//		this(source, create(next));
//	}
//
	static <T> Function<Throwable, Publisher<? extends T>> create(final Publisher<? extends T> next) {
		Objects.requireNonNull(next, "next");
		return new Function<Throwable, Publisher<? extends T>>() {
			@Override
			public Publisher<? extends T> apply(Throwable e) {
				return next;
			}
		};
	}
	
	public static <T> FluxResume<T> create(Publisher<? extends T> source, Publisher<? extends T> other) {
		return new FluxResume<>(source, create(other));
	}
	
	public FluxResume(Publisher<? extends T> source,
						   Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory) {
		super(source);
		this.nextFactory = Objects.requireNonNull(nextFactory, "nextFactory");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new ResumeSubscriber<>(s, nextFactory));
	}

	static final class ResumeSubscriber<T> extends SubscriberMultiSubscription<T, T> implements Connectable {

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
					p = nextFactory.apply(t);
				} catch (Throwable e) {
					Throwable _e = Exceptions.unwrap(e);
					_e.addSuppressed(t);
					Exceptions.throwIfFatal(e);
					subscriber.onError(_e);
					return;
				}
				if (p == null) {
					NullPointerException t2 = new NullPointerException("The nextFactory returned a null Publisher");
					t2.addSuppressed(t);
					subscriber.onError(t2);
				} else {
					p.subscribe(this);
				}
			} else {
				subscriber.onError(t);
			}
		}

		@Override
		public Object connectedInput() {
			return nextFactory;
		}

		@Override
		public Object connectedOutput() {
			return null;
		}
	}
}
