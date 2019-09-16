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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Waits for a Mono source to terminate or produce a value, in which case the value is
 * mapped to a Publisher used as a delay: its termination will trigger the actual emission
 * of the value downstream. If the Mono source didn't produce a value, terminate with the
 * same signal (empty completion or error).
 *
 * @param <T> the value type
 *
 * @author Simon Baslé
 */
final class MonoDelayUntil<T> extends Mono<T> implements Scannable,
                                                         OptimizableOperator<T, T> {

	final Mono<T> source;

	Function<? super T, ? extends Publisher<?>>[] otherGenerators;

	@Nullable
	final OptimizableOperator<?, T> optimizableOperator;

	@SuppressWarnings("unchecked")
	MonoDelayUntil(Mono<T> monoSource,
			Function<? super T, ? extends Publisher<?>> triggerGenerator) {
		this.source = Objects.requireNonNull(monoSource, "monoSource");
		this.otherGenerators = new Function[] { Objects.requireNonNull(triggerGenerator, "triggerGenerator")};
		this.optimizableOperator = source instanceof OptimizableOperator ? (OptimizableOperator) source : null;
	}

	MonoDelayUntil(Mono<T> monoSource,
			Function<? super T, ? extends Publisher<?>>[] triggerGenerators) {
		this.source = Objects.requireNonNull(monoSource, "monoSource");
		this.otherGenerators = triggerGenerators;
		this.optimizableOperator = source instanceof OptimizableOperator ? (OptimizableOperator) source : null;
	}

	/**
	 * Add a trigger generator to wait for.
	 * @param delayError the delayError parameter for the trigger being added, ignored if already true
	 * @param triggerGenerator the new trigger to add to the copy of the operator
	 * @return a new {@link MonoDelayUntil} instance with same source but additional trigger generator
	 */
	@SuppressWarnings("unchecked")
	MonoDelayUntil<T> copyWithNewTriggerGenerator(boolean delayError,
			Function<? super T, ? extends Publisher<?>> triggerGenerator) {
		Objects.requireNonNull(triggerGenerator, "triggerGenerator");
		Function<? super T, ? extends Publisher<?>>[] oldTriggers = this.otherGenerators;
		Function<? super T, ? extends Publisher<?>>[] newTriggers = new Function[oldTriggers.length + 1];
		System.arraycopy(oldTriggers, 0, newTriggers, 0, oldTriggers.length);
		newTriggers[oldTriggers.length] = triggerGenerator;
		return new MonoDelayUntil<>(this.source, newTriggers);
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		source.subscribe(subscribeOrReturn(actual));
	}

	@Override
	public final CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		DelayUntilCoordinator<T> parent = new DelayUntilCoordinator<>(actual, otherGenerators);
		actual.onSubscribe(parent);

		return parent;
	}

	@Override
	public final CorePublisher<? extends T> source() {
		return source;
	}

	@Override
	public final OptimizableOperator<?, ? extends T> nextOptimizableSource() {
		return optimizableOperator;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		return null; //no particular key to be represented, still useful in hooks
	}

	static final class DelayUntilCoordinator<T>
			extends Operators.MonoSubscriber<T, T> {

		static final DelayUntilTrigger[] NO_TRIGGER = new DelayUntilTrigger[0];

		final int                                                n;
		final Function<? super T, ? extends Publisher<?>>[] otherGenerators;

		volatile int done;
		static final AtomicIntegerFieldUpdater<DelayUntilCoordinator> DONE =
				AtomicIntegerFieldUpdater.newUpdater(DelayUntilCoordinator.class, "done");

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<DelayUntilCoordinator, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(DelayUntilCoordinator.class, Subscription.class, "s");

		DelayUntilTrigger[] triggerSubscribers;

		DelayUntilCoordinator(CoreSubscriber<? super T> subscriber,
				Function<? super T, ? extends Publisher<?>>[] otherGenerators) {
			super(subscriber);
			this.otherGenerators = otherGenerators;
			//don't consider the source as this is only used from when there is a value
			this.n = otherGenerators.length;
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
			if (value == null && state < HAS_REQUEST_HAS_VALUE) {
				actual.onComplete();
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done == n;

			return super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(triggerSubscribers);
		}


		@SuppressWarnings("unchecked")
		void subscribeNextTrigger(T value, int triggerIndex) {
			if (triggerSubscribers == NO_TRIGGER) {
				triggerSubscribers = new DelayUntilTrigger[otherGenerators.length];
			}

			Function<? super T, ? extends Publisher<?>> generator = otherGenerators[triggerIndex];

			Publisher<?> p;

			try {
				p = generator.apply(value);
			}
			catch (Throwable t) {
				onError(t);
				return;
			}

			DelayUntilTrigger triggerSubscriber = new DelayUntilTrigger<>(this);

			this.triggerSubscribers[triggerIndex] = triggerSubscriber;
			p.subscribe(triggerSubscriber);
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
						//this is ok as the composite created by multiple is never a singleton
						compositeError.addSuppressed(e);
					} else
					if (error != null) {
						compositeError = Exceptions.multiple(error, e);
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
		public Context currentContext() {
			return parent.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.PARENT) return s;
			if (key == Attr.ACTUAL) return parent;
			if (key == Attr.ERROR) return error;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;

			return null;
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
		public void onNext(Object t) {
			//NO-OP
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			if (DelayUntilCoordinator.DONE.getAndSet(parent, parent.n) != parent.n) {
				parent.cancel();
				parent.actual.onError(t);
			}
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
