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
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;

/**
 * Waits for a Mono source to terminate or produce a value, in which case the value is
 * mapped to a Publisher used as a delay: its first production or termination will trigger
 * the actual emission of the value downstream. If the Mono source didn't produce a value,
 * terminate with the same signal (empty completion or error).
 *
 * @param <T> the value type
 *
 * @author Simon Baslé
 */
final class MonoDelayUntil<T> extends Mono<T> {

	final boolean delayError;

	final Mono<T> source;

	Function<? super T, ? extends Publisher<?>>[] otherGenerators;

	MonoDelayUntil(boolean delayError,
			Mono<T> monoSource,
			Function<? super T, ? extends Publisher<?>> triggerGenerator) {
		this.delayError = delayError;
		this.source = Objects.requireNonNull(monoSource, "monoSource");
		this.otherGenerators = new Function[] { Objects.requireNonNull(triggerGenerator, "triggerGenerator")};
	}

	private MonoDelayUntil(boolean delayError,
			Mono<T> monoSource,
			Function<? super T, ? extends Publisher<?>>[] triggerGenerators) {
		this.delayError = delayError;
		this.source = Objects.requireNonNull(monoSource, "monoSource");
		this.otherGenerators = triggerGenerators;
	}

	/**
	 * Add a trigger generator to wait for.
	 * @param delayError the delayError parameter for the trigger being added, ignored if already true
	 * @param triggerGenerator the new trigger to add to the copy of the operator
	 * @return a new {@link MonoDelayUntil} instance with same source but additional trigger generator
	 */
	MonoDelayUntil<T> copyWithNewTriggerGenerator(boolean delayError,
			Function<? super T, ? extends Publisher<?>> triggerGenerator) {
		Objects.requireNonNull(triggerGenerator, "triggerGenerator");
		Function<? super T, ? extends Publisher<?>>[] oldTriggers = this.otherGenerators;
		Function<? super T, ? extends Publisher<?>>[] newTriggers = new Function[oldTriggers.length + 1];
		System.arraycopy(oldTriggers, 0, newTriggers, 0, oldTriggers.length);
		newTriggers[oldTriggers.length] = triggerGenerator;
		return new MonoDelayUntil<T>(this.delayError ? true : delayError, this.source, newTriggers);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		DelayUntilCoordinator<T> parent = new DelayUntilCoordinator<>(s,
				delayError, otherGenerators);
		s.onSubscribe(parent);
		source.subscribe(parent);
	}

	static final class DelayUntilCoordinator<T>
			extends Operators.MonoSubscriber<T, T> {

		static final DelayUntilTrigger[] NO_TRIGGER = new DelayUntilTrigger[0];

		final int                                                n;
		final boolean                                            delayError;
		final Function<? super T, ? extends Publisher<?>>[] otherGenerators;

		volatile int done;
		static final AtomicIntegerFieldUpdater<DelayUntilCoordinator> DONE =
				AtomicIntegerFieldUpdater.newUpdater(DelayUntilCoordinator.class, "done");

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<DelayUntilCoordinator, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(DelayUntilCoordinator.class, Subscription.class, "s");

		DelayUntilTrigger[] triggerSubscribers;

		DelayUntilCoordinator(Subscriber<? super T> subscriber,
				boolean delayError,
				Function<? super T, ? extends Publisher<?>>[] otherGenerators) {
			super(subscriber);
			this.otherGenerators = otherGenerators;
			//don't consider the source as this is only used from when there is a value
			this.n = otherGenerators.length;

			this.delayError = delayError;
			triggerSubscribers = NO_TRIGGER;
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
				setValue(t);
				subscribeNextTrigger(t, done);
			}
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (value == null) {
				actual.onComplete();
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.TERMINATED) return done == n;
			if (key == BooleanAttr.DELAY_ERROR) return delayError;

			return super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(triggerSubscribers);
		}

		private void subscribeNextTrigger(T value, int triggerIndex) {
			if (triggerSubscribers == NO_TRIGGER) {
				triggerSubscribers = new DelayUntilTrigger[otherGenerators.length];
			}

			Function<? super T, ? extends Publisher<?>> generator = otherGenerators[triggerIndex];
			Publisher<?> p = generator.apply(value);
			DelayUntilTrigger triggerSubscriber = new DelayUntilTrigger(this);

			this.triggerSubscribers[triggerIndex] = triggerSubscriber;
			p.subscribe(triggerSubscriber);
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
			int nextIndex = DONE.incrementAndGet(this);
			if (nextIndex != n) {
				subscribeNextTrigger(this.value, nextIndex);
				return;
			}

			Throwable error = null;
			Throwable compositeError = null;

			//check for errors in the triggers
			for (int i = 0; i < n; i++) {
				DelayUntilTrigger mt = triggerSubscribers[i];
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
			} else {
				//emit the value downstream
				complete(this.value);
			}
		}

		@Override
		public void cancel() {
			if (!isCancelled()) {
				super.cancel();
				//source is always cancellable...
				Operators.terminate(S, this);
				//...but triggerSubscribers could be partially initialized
				for (int i = 0; i < triggerSubscribers.length; i++) {
					DelayUntilTrigger ts = triggerSubscribers[i];
					if (ts != null) ts.cancel();
				}
			}
		}
	}

	static final class DelayUntilTrigger<T> implements InnerConsumer<T> {

		final DelayUntilCoordinator<?> parent;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<DelayUntilTrigger, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(DelayUntilTrigger.class, Subscription.class, "s");

		boolean done;
		volatile Throwable error;

		DelayUntilTrigger(DelayUntilCoordinator<?> parent) {
			this.parent = parent;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == BooleanAttr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == ScannableAttr.PARENT) return s;
			if (key == ScannableAttr.ACTUAL) return parent;
			if (key == ThrowableAttr.ERROR) return error;
			if (key == IntAttr.PREFETCH) return Integer.MAX_VALUE;

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Integer.MAX_VALUE);
			} else {
				s.cancel();
			}
		}

		@Override
		public void onNext(Object t) {
			//NO-OP
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
