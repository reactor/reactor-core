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

	final boolean delayError;

	final Mono<T> source;

	Publisher<?>[] others;

	final Function<? super T, ? extends R> mapper;

	public MonoUntilOther(boolean delayError,
			Mono<T> monoSource,
			Publisher<?> triggerPublisher,
			Function<? super T, ? extends R> mapper) {
		this.delayError = delayError;
		this.source = Objects.requireNonNull(monoSource, "monoSource");
		this.others = new Publisher[] { Objects.requireNonNull(triggerPublisher, "triggerPublisher")};
		this.mapper = Objects.requireNonNull(mapper, "mapper");
	}

	/**
	 * Add a trigger to wait for, without any additional transformation (as there would
	 * be no way of detecting said transformation is compatible with the array of sources).
	 * @param trigger
	 */
	public void addTrigger(Publisher<?> trigger) {
		Publisher<?>[] oldTriggers = this.others;
		this.others = new Publisher[oldTriggers.length + 1];
		System.arraycopy(oldTriggers, 0, this.others, 0, oldTriggers.length);
		this.others[oldTriggers.length] = trigger;
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {
		MonoUntilOtherCoordinator<T, R> parent = new MonoUntilOtherCoordinator<>(s, delayError, mapper, others.length + 1);
		s.onSubscribe(parent);
		parent.subscribe(source, others);
	}

	static final class MonoUntilOtherCoordinator<T, R>
			extends Operators.MonoSubscriber<T, R> implements Subscription {

		final int                               n;
		final boolean                           delayError;
		final Function<? super T, ? extends R>  mapper;
		final MonoUntilOtherSourceSubscriber<T> sourceSubscriber;
		final MonoUntilOtherTriggerSubscriber[] triggerSubscribers;

		volatile int done;
		static final AtomicIntegerFieldUpdater<MonoUntilOtherCoordinator> DONE =
				AtomicIntegerFieldUpdater.newUpdater(MonoUntilOtherCoordinator.class, "done");

		MonoUntilOtherCoordinator(Subscriber<? super R> subscriber,
				boolean delayError,
				Function<? super T, ? extends R> mapper,
				int n) {
			super(subscriber);
			this.n = n;
			this.delayError = delayError;
			this.mapper = mapper;
			sourceSubscriber = new MonoUntilOtherSourceSubscriber<>(this);
			triggerSubscribers = new MonoUntilOtherTriggerSubscriber[n - 1];
		}

		void subscribe(Publisher<T> source, Publisher<?>[] triggers) {
			if (triggers.length != triggerSubscribers.length) {
				throw new IllegalArgumentException(triggerSubscribers.length + " triggers required");
			}
			source.subscribe(sourceSubscriber);
			for (int i = 0; i < triggerSubscribers.length; i++) {
				Publisher p = triggers[i];
				boolean cancelOnTriggerValue = !(p instanceof Mono);
				MonoUntilOtherTriggerSubscriber triggerSubscriber = new MonoUntilOtherTriggerSubscriber(this, cancelOnTriggerValue);
				this.triggerSubscribers[i] = triggerSubscriber;
				p.subscribe(triggerSubscriber);
			}
		}

		void signalError(Throwable t) {
			if (delayError) {
				signal();
			} else {
				if (DONE.getAndSet(this, n) != n) {
					cancel();
					actual.onError(t);
				}
			}
		}

		void signal() {
			if (DONE.incrementAndGet(this) != n) {
				return;
			}

			T o = null;
			Throwable error = null;
			Throwable compositeError = null;
			boolean sourceEmpty = false;

			//check if source produced value or error
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
					sourceEmpty = true;
				}
			}

			//check for errors in the triggers
			for (int i = 0; i < n - 1; i++) {
				MonoUntilOtherTriggerSubscriber mt = triggerSubscribers[i];
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
			}

			if (compositeError != null) {
				actual.onError(compositeError);
			}
			else if (error != null) {
				actual.onError(error);
			}
			else if (sourceEmpty) {
				actual.onComplete();
			} else {
				//apply the transformation to the source
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
				//sourceSubscriber is eagerly created so always cancellable...
				sourceSubscriber.cancel();
				//...but triggerSubscribers could be partially initialized
				for (int i = 0; i < triggerSubscribers.length; i++) {
					MonoUntilOtherTriggerSubscriber ts = triggerSubscribers[i];
					if (ts != null) ts.cancel();
				}
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
		final boolean cancelOnTriggerValue;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MonoUntilOtherTriggerSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(MonoUntilOtherTriggerSubscriber.class, Subscription.class, "s");

		boolean done;
		Throwable error;

		public MonoUntilOtherTriggerSubscriber(MonoUntilOtherCoordinator<?, ?> parent,
				boolean cancelOnTriggerValue) {
			this.parent = parent;
			this.cancelOnTriggerValue = cancelOnTriggerValue;
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
				if (cancelOnTriggerValue) {
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
