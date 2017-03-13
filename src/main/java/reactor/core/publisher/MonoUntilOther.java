/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Waits for a Mono source to produce a value or terminate, as well as a Publisher source
 * for which first production or termination will be used as a trigger. If the Mono source
 * produced a value, emit that value after the Mono has completed and the Publisher source
 * has either emitted once or completed. Otherwise terminate empty.
 *
 * @param <T> the source value type
 * @param <R> the transformed value type
 *
 * @author Simon Baslé
 */
final class MonoUntilOther<T, R> extends Mono<R> {

	final boolean cancelOnTriggerValue;

	final boolean delayError;

	final Mono<T> source;

	final Publisher<?> other;

	final Function<? super T, ? extends R> mapper;

	public MonoUntilOther(boolean delayError,
			Mono<T> monoSource,
			Publisher<?> triggerPublisher,
			Function<? super T, ? extends R> mapper) {
		this.delayError = delayError;
		this.source = Objects.requireNonNull(monoSource, "monoSource");
		this.other = Objects.requireNonNull(triggerPublisher, "triggerPublisher");
		this.mapper = Objects.requireNonNull(mapper, "mapper");
		this.cancelOnTriggerValue = !(other instanceof Mono);
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {
		MonoUntilOtherCoordinator<T, R> parent = new MonoUntilOtherCoordinator<>(s, delayError, mapper, cancelOnTriggerValue);
		s.onSubscribe(parent);
		parent.subscribe(source, other);
	}

	static final class MonoUntilOtherCoordinator<T, R>
			extends Operators.MonoSubscriber<T, R> implements Subscription {

		final boolean                           cancelOnTriggerValue;
		final boolean                           delayError;
		final Function<? super T, ? extends R>  mapper;
		final MonoUntilOtherSourceSubscriber<T> sourceSubscriber;
		final MonoUntilOtherTriggerSubscriber   triggerSubscriber;

		volatile int done;
		static final AtomicIntegerFieldUpdater<MonoUntilOtherCoordinator> DONE =
				AtomicIntegerFieldUpdater.newUpdater(MonoUntilOtherCoordinator.class, "done");

		MonoUntilOtherCoordinator(Subscriber<? super R> subscriber,
				boolean delayError,
				Function<? super T, ? extends R> mapper,
				boolean cancelOnFirstTriggerValue) {
			super(subscriber);
			this.cancelOnTriggerValue = cancelOnFirstTriggerValue;
			this.delayError = delayError;
			this.mapper = mapper;
			sourceSubscriber = new MonoUntilOtherSourceSubscriber<>(this);
			triggerSubscriber = new MonoUntilOtherTriggerSubscriber(this);
		}

		void subscribe(Publisher<T> source, Publisher<?> trigger) {
			source.subscribe(sourceSubscriber);
			trigger.subscribe(triggerSubscriber);
		}

		void signalError(Throwable t) {
			if (delayError) {
				signal();
			} else {
				if (DONE.getAndSet(this, 2) != 2) {
					cancel();
					actual.onError(t);
				}
			}
		}

		void signal() {
			if (DONE.incrementAndGet(this) != 2) {
				return;
			}

			T o = null;
			Throwable error = null;
			Throwable compositeError = null;
			boolean hasEmpty = false;

			MonoUntilOtherSourceSubscriber<T> ms = sourceSubscriber;
			T v = ms.value;
			if (v != null) {
				o = v;
			} else {
				Throwable e = ms.error;
				if (e != null) {
					if (compositeError != null) {
						compositeError.addSuppressed(e);
					} else if (error != null) {
						compositeError = new Throwable("Multiple errors");
						compositeError.addSuppressed(error);
						compositeError.addSuppressed(e);
					} else {
						error = e;
					}
				} else {
					hasEmpty = true;
				}
			}

			MonoUntilOtherTriggerSubscriber mt = triggerSubscriber;
			Throwable e = mt.error;
			if (e != null) {
				if (compositeError != null) {
					compositeError.addSuppressed(e);
				} else
				if (error != null) {
					compositeError = new Throwable("Multiple errors");
					compositeError.addSuppressed(error);
					compositeError.addSuppressed(e);
				} else {
					error = e;
				}
				//else the trigger publisher was empty, but we'll ignore that
			}

			if (compositeError != null) {
				actual.onError(compositeError);
			} else
			if (error != null) {
				actual.onError(error);
			} else
			if (hasEmpty) {
				actual.onComplete();
			} else {
				R r;
				try {
					r = Objects.requireNonNull(mapper.apply(o), "mapper produced a null value");
				}
				catch (Throwable t) {
					actual.onError(Operators.onOperatorError(null, t, o));
					return;
				}
				complete(r);
			}
		}

		@Override
		public void cancel() {
			if (!isCancelled()) {
				super.cancel();
				sourceSubscriber.cancel();
				triggerSubscriber.cancel();
			}
		}
	}

	static final class MonoUntilOtherSourceSubscriber<T> implements Subscriber<T> {

		final MonoUntilOtherCoordinator<T, ?> parent;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MonoUntilOtherSourceSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(MonoUntilOtherSourceSubscriber.class, Subscription.class, "s");

		T value;
		Throwable error;

		public MonoUntilOtherSourceSubscriber(MonoUntilOtherCoordinator<T, ?> parent) {
			this.parent = parent;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			} else {
				s.cancel();
			}
		}

		@Override
		public void onNext(T t) {
			if (value == null) {
				value = t;
				parent.signal();
			}
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			parent.signalError(t);
		}

		@Override
		public void onComplete() {
			if (value == null) {
				parent.signal();
			}
		}

		void cancel() {
			Operators.terminate(S, this);
		}
	}

	static final class MonoUntilOtherTriggerSubscriber implements Subscriber<Object> {

		final MonoUntilOtherCoordinator<?, ?> parent;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MonoUntilOtherTriggerSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(MonoUntilOtherTriggerSubscriber.class, Subscription.class, "s");

		boolean done;
		Throwable error;

		public MonoUntilOtherTriggerSubscriber(MonoUntilOtherCoordinator<?, ?> parent) {
			this.parent = parent;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(1);
			} else {
				s.cancel();
			}
		}

		@Override
		public void onNext(Object t) {
			if (!done) {
				done = true;
				parent.signal();
				if (parent.cancelOnTriggerValue) {
					s.cancel();
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			parent.signalError(t);
		}

		@Override
		public void onComplete() {
			if (!done) {
				done = true;
				parent.signal();
			}
		}

		void cancel() {
			Operators.terminate(S, this);
		}
	}
}
