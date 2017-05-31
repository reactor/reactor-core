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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;


/**
 * Waits for a Mono source to produce a value or terminate, as well as a Publisher source
 * for which first production or termination will be used as a trigger. If the Mono source
 * produced a value, emit that value after the Mono has completed and the Publisher source
 * has either emitted once or completed. Otherwise terminate empty.
 *
 * @param <T> the value type
 *
 * @author Simon Baslé
 */
final class MonoUntilOther<T> extends Mono<T> {

	final boolean delayError;

	final Mono<T> source;

	Publisher<?>[] others;

	MonoUntilOther(boolean delayError,
			Mono<T> monoSource,
			Publisher<?> triggerPublisher) {
		this.delayError = delayError;
		this.source = Objects.requireNonNull(monoSource, "monoSource");
		this.others = new Publisher[] { Objects.requireNonNull(triggerPublisher, "triggerPublisher")};
	}

	private MonoUntilOther(boolean delayError,
			Mono<T> monoSource,
			Publisher<?>[] triggerPublishers) {
		this.delayError = delayError;
		this.source = Objects.requireNonNull(monoSource, "monoSource");
		this.others = triggerPublishers;
	}

	/**
	 * Add a trigger to wait for.
	 * @param trigger
	 * @return a new {@link MonoUntilOther} instance with same source but additional trigger
	 */
	MonoUntilOther<T> copyWithNewTrigger(Publisher<?> trigger) {
		Objects.requireNonNull(trigger, "trigger");
		Publisher<?>[] oldTriggers = this.others;
		Publisher<?>[] newTriggers = new Publisher[oldTriggers.length + 1];
		System.arraycopy(oldTriggers, 0, newTriggers, 0, oldTriggers.length);
		newTriggers[oldTriggers.length] = trigger;
		return new MonoUntilOther<T>(this.delayError, this.source, newTriggers);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		UntilOtherCoordinator<T> parent = new UntilOtherCoordinator<>(s,
				delayError,
				others.length + 1);
		s.onSubscribe(parent);
		parent.subscribe(source, others);
	}

	static final class UntilOtherCoordinator<T>
			extends Operators.MonoSubscriber<T, T> {

		final int                      n;
		final boolean                  delayError;
		final UntilOtherSource<T> sourceSubscriber;
		final UntilOtherTrigger[] triggerSubscribers;

		volatile int done;
		static final AtomicIntegerFieldUpdater<UntilOtherCoordinator> DONE =
				AtomicIntegerFieldUpdater.newUpdater(UntilOtherCoordinator.class, "done");

		UntilOtherCoordinator(Subscriber<? super T> subscriber,
				boolean delayError,
				int n) {
			super(subscriber);
			this.n = n;

			this.delayError = delayError;
			sourceSubscriber = new UntilOtherSource<>(this);
			triggerSubscribers = new UntilOtherTrigger[n - 1];
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.TERMINATED) return done == n;
			if (key == ScannableAttr.PARENT) return sourceSubscriber;
			if (key == BooleanAttr.DELAY_ERROR) return delayError;

			return super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(triggerSubscribers);
		}

		@SuppressWarnings("unchecked")
		void subscribe(Publisher<T> source, Publisher<?>[] triggers) {
			if (triggers.length != triggerSubscribers.length) {
				throw new IllegalArgumentException(triggerSubscribers.length + " triggers required");
			}
			source.subscribe(sourceSubscriber);
			for (int i = 0; i < triggerSubscribers.length; i++) {
				Publisher<?> p = triggers[i];
				boolean cancelOnTriggerValue = !(p instanceof Mono);
				UntilOtherTrigger triggerSubscriber = new UntilOtherTrigger(this, cancelOnTriggerValue);
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
			UntilOtherSource<T> ms = sourceSubscriber;
			T v = ms.value;
			if (v != null) {
				o = v;
			} else {
				Throwable e = ms.error;
				if (e != null) {
					error = e; //at this point always the first error evaluated
				} else {
					sourceEmpty = true;
				}
			}

			//check for errors in the triggers
			for (int i = 0; i < n - 1; i++) {
				UntilOtherTrigger mt = triggerSubscribers[i];
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
				//emit the value downstream
				complete(o);
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
					UntilOtherTrigger ts = triggerSubscribers[i];
					if (ts != null) ts.cancel();
				}
			}
		}
	}

	static final class UntilOtherSource<T> implements InnerConsumer<T> {

		final UntilOtherCoordinator<T> parent;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<UntilOtherSource, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(UntilOtherSource.class, Subscription.class, "s");

		T value;
		Throwable error;

		public UntilOtherSource(UntilOtherCoordinator<T> parent) {
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

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == ThrowableAttr.ERROR) return error;
			if (key == BooleanAttr.TERMINATED) return error != null || value != null;
			if (key == BooleanAttr.CANCELLED) return s == Operators.cancelledSubscription();

			return null;
		}

		void cancel() {
			Operators.terminate(S, this);
		}
	}

	static final class UntilOtherTrigger<T> implements InnerConsumer<T> {

		final UntilOtherCoordinator<?> parent;
		final boolean                       cancelOnTriggerValue;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<UntilOtherTrigger, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(UntilOtherTrigger.class, Subscription.class, "s");

		boolean done;
		Throwable error;

		UntilOtherTrigger(UntilOtherCoordinator<?> parent,
				boolean cancelOnTriggerValue) {
			this.parent = parent;
			this.cancelOnTriggerValue = cancelOnTriggerValue;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == ScannableAttr.PARENT) return s;
			if (key == ScannableAttr.ACTUAL) return parent;
			if (key == ThrowableAttr.ERROR) return error;

			return null;
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
