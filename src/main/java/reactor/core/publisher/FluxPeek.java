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

import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.publisher.FluxPeekFuseable.PeekConditionalSubscriber;
import reactor.core.publisher.FluxPeekFuseable.PeekFuseableSubscriber;
import reactor.core.Exceptions;

/**
 * Peek into the lifecycle events and signals of a sequence.
 * <p>
 * <p>
 * The callbacks are all optional.
 * <p>
 * <p>
 * Crashes by the lambdas are ignored.
 *
 * @param <T> the value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxPeek<T> extends FluxSource<T, T> implements FluxPeekHelper<T> {

	final Consumer<? super Subscription> onSubscribeCall;

	final Consumer<? super T> onNextCall;

	final Consumer<? super Throwable> onErrorCall;

	final Runnable onCompleteCall;

	final Runnable onAfterTerminateCall;

	final LongConsumer onRequestCall;

	final Runnable onCancelCall;

	public FluxPeek(Publisher<? extends T> source, Consumer<? super Subscription> onSubscribeCall,
			Consumer<? super T> onNextCall,
			Consumer<? super Throwable> onErrorCall,
			Runnable onCompleteCall,
			Runnable onAfterTerminateCall,
			LongConsumer onRequestCall,
			Runnable onCancelCall) {
		super(source);
		this.onSubscribeCall = onSubscribeCall;
		this.onNextCall = onNextCall;
		this.onErrorCall = onErrorCall;
		this.onCompleteCall = onCompleteCall;
		this.onAfterTerminateCall = onAfterTerminateCall;
		this.onRequestCall = onRequestCall;
		this.onCancelCall = onCancelCall;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (source instanceof Fuseable) {
			source.subscribe(new PeekFuseableSubscriber<>(s, this));
			return;
		}
		if (s instanceof ConditionalSubscriber) {
			@SuppressWarnings("unchecked") // javac, give reason to suppress because inference anomalies
					ConditionalSubscriber<T> s2 = (ConditionalSubscriber<T>) s;
			source.subscribe(new PeekConditionalSubscriber<>(s2, this));
			return;
		}
		source.subscribe(new PeekSubscriber<>(s, this));
	}

	static final class PeekSubscriber<T> implements Subscriber<T>, Subscription, Receiver, Producer {

		final Subscriber<? super T> actual;

		final FluxPeekHelper<T> parent;

		Subscription s;

		boolean done;

		public PeekSubscriber(Subscriber<? super T> actual, FluxPeekHelper<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if(parent.onRequestCall() != null) {
				try {
					parent.onRequestCall().accept(n);
				}
				catch (Throwable e) {
					onError(Exceptions.mapOperatorError(s, e));
					return;
				}
			}
			s.request(n);
		}

		@Override
		public void cancel() {
			if(parent.onCancelCall() != null) {
				try {
					parent.onCancelCall().run();
				}
				catch (Throwable e) {
					onError(Exceptions.mapOperatorError(s, e));
					return;
				}
			}
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if(parent.onSubscribeCall() != null) {
				try {
					parent.onSubscribeCall().accept(s);
				}
				catch (Throwable e) {
					Operators.error(actual, Exceptions.mapOperatorError(s, e));
					return;
				}
			}
			this.s = s;
			actual.onSubscribe(this);
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}
			if(parent.onNextCall() != null) {
				try {
					parent.onNextCall().accept(t);
				}
				catch (Throwable e) {
					onError(Exceptions.mapOperatorError(s, e, t));
					return;
				}
			}

			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
				return;
			}
			done = true;
			if(parent.onErrorCall() != null) {
				parent.onErrorCall().accept(t);
			}

			actual.onError(t);

			if(parent.onAfterTerminateCall() != null) {
				try {
					parent.onAfterTerminateCall().run();
				}
				catch (Throwable e) {
					Throwable _e = Exceptions.mapOperatorError(null, e, t);
					e.addSuppressed(t);
					if(parent.onErrorCall() != null) {
						parent.onErrorCall().accept(_e);
					}
					actual.onError(_e);
				}
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			if(parent.onCompleteCall() != null) {
				try {
					parent.onCompleteCall().run();
				}
				catch (Throwable e) {
					onError(Exceptions.mapOperatorError(e));
					return;
				}
			}

			actual.onComplete();

			if(parent.onAfterTerminateCall() != null) {
				try {
					parent.onAfterTerminateCall().run();
				}
				catch (Throwable e) {
					Throwable _e = Exceptions.mapOperatorError(e);
					if(parent.onErrorCall() != null) {
						parent.onErrorCall().accept(_e);
					}
					actual.onError(_e);
				}
			}
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object upstream() {
			return s;
		}
	}

	@Override
	public Consumer<? super Subscription> onSubscribeCall() {
		return onSubscribeCall;
	}

	@Override
	public Consumer<? super T> onNextCall() {
		return onNextCall;
	}

	@Override
	public Consumer<? super Throwable> onErrorCall() {
		return onErrorCall;
	}

	@Override
	public Runnable onCompleteCall() {
		return onCompleteCall;
	}

	@Override
	public Runnable onAfterTerminateCall() {
		return onAfterTerminateCall;
	}

	@Override
	public LongConsumer onRequestCall() {
		return onRequestCall;
	}

	@Override
	public Runnable onCancelCall() {
		return onCancelCall;
	}

}