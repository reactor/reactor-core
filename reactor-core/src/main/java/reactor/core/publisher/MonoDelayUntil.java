/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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
		if (source instanceof OptimizableOperator) {
			@SuppressWarnings("unchecked")
			OptimizableOperator<?, T> optimSource = (OptimizableOperator<?, T>) source;
			this.optimizableOperator = optimSource;
		}
		else {
			this.optimizableOperator = null;
		}
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
		try {
			source.subscribe(subscribeOrReturn(actual));
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return;
		}
	}

	@Override
	public final CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) throws Throwable {
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
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null; //no particular key to be represented, still useful in hooks
	}

	static final class DelayUntilCoordinator<T> implements InnerOperator<T, T> {

		final Function<? super T, ? extends Publisher<?>>[] otherGenerators;
		final CoreSubscriber<? super T> actual;

		int index;

		T value;
		boolean done;

		Subscription s;
		DelayUntilTrigger<?> triggerSubscriber;

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<DelayUntilCoordinator, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(DelayUntilCoordinator.class, Throwable.class, "error");

		volatile int state;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<DelayUntilCoordinator> STATE =
				AtomicIntegerFieldUpdater.newUpdater(DelayUntilCoordinator.class, "state");

		static final int HAS_SUBSCRIPTION = 0b00000000000000000000000000000001;
		static final int HAS_INNER        = 0b00000000000000000000000000000010;
		static final int HAS_REQUEST      = 0b00000000000000000000000000000100;
		static final int HAS_VALUE        = 0b00000000000000000000000000001000;
		static final int TERMINATED       = 0b10000000000000000000000000000000;

		DelayUntilCoordinator(CoreSubscriber<? super T> subscriber,
				Function<? super T, ? extends Publisher<?>>[] otherGenerators) {
			this.actual = subscriber;
			this.otherGenerators = otherGenerators;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				int previousState = markHasSubscription();
				if (isTerminated(previousState)) {
					s.cancel();
					return;
				}

				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (this.done) {
				Operators.onDiscard(t, this.actual.currentContext());
				return;
			}

			this.value = t;
			subscribeNextTrigger();
		}

		@Override
		public void onError(Throwable t) {
			if (this.done) {
				Operators.onErrorDropped(t, this.actual.currentContext());
				return;
			}

			this.done = true;

			if (this.value == null) {
				this.actual.onError(t);
				return;
			}

			if (!Exceptions.addThrowable(ERROR, this, t)) {
				Operators.onErrorDropped(t, this.actual.currentContext());
				return;
			}

			final int previousState = markTerminated();
			if (isTerminated(previousState)) {
				return;
			}

			if (hasInner(previousState)) {
				Operators.onDiscard(this.value, this.actual.currentContext());
				this.triggerSubscriber.cancel();
			}

			final Throwable e = Exceptions.terminate(ERROR, this);
			//noinspection ConstantConditions
			this.actual.onError(e);
		}

		@Override
		public void onComplete() {
			if (this.done) {
				return;
			}

			if (this.value == null) {
				this.done = true;
				this.actual.onComplete();
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				final int previousState = markHasRequest();

				if (isTerminated(previousState)) {
					return;
				}

				if (hasRequest(previousState)) {
					return;
				}

				if (hasValue(previousState)) {
					this.done = true;
					final CoreSubscriber<? super T> actual = this.actual;
					final T v = this.value;

					actual.onNext(v);
					actual.onComplete();
				}
			}
		}

		@Override
		public void cancel() {
			final int previousState = markTerminated();

			if (isTerminated(previousState)) {
				return;
			}

			final Throwable t = Exceptions.terminate(ERROR, this);
			if (t != null) {
				Operators.onErrorDropped(t, this.actual.currentContext());
			}

			if (hasSubscription(previousState)) {
				this.s.cancel();
			}

			if (hasInner(previousState)) {
				Operators.onDiscard(this.value, this.actual.currentContext());

				this.triggerSubscriber.cancel();
		    }
		}

		@SuppressWarnings({"unchecked", "rawtypes"})
		void subscribeNextTrigger() {
			final Function<? super T, ? extends Publisher<?>> generator =
					this.otherGenerators[this.index];

			final Publisher<?> p;

			try {
				p = generator.apply(this.value);
				Objects.requireNonNull(p, "mapper returned null value");
			}
			catch (Throwable t) {
				onError(t);
				return;
			}

			DelayUntilTrigger triggerSubscriber = this.triggerSubscriber;
			if (triggerSubscriber == null) {
				triggerSubscriber = new DelayUntilTrigger<>(this);
				this.triggerSubscriber = triggerSubscriber;
			}

			p.subscribe(triggerSubscriber);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return isTerminated(this.state) && !this.done;
			if (key == Attr.TERMINATED) return isTerminated(this.state) && this.done;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			final DelayUntilTrigger<?> subscriber = this.triggerSubscriber;
			return subscriber == null ? Stream.empty() : Stream.of(subscriber);
		}

		/**
		 * Sets flag has subscription to indicate that we have already received
		 * subscription from the value upstream
		 *
		 * @return previous state
		 */
		int markHasSubscription() {
			for (;;) {
				final int state = this.state;

				if (isTerminated(state)) {
					return TERMINATED;
				}

				if (STATE.compareAndSet(this, state, state | HAS_SUBSCRIPTION)) {
					return state;
				}
			}
		}

		/**
		 * Sets {@link #HAS_REQUEST} flag which indicates that there is a demand from
		 * the downstream
		 *
		 * @return previous state
		 */
		int markHasRequest() {
			for (; ; ) {
				final int state = this.state;
				if (isTerminated(state)) {
					return TERMINATED;
				}

				if (hasRequest(state)) {
					return state;
				}

				final int nextState;
				if (hasValue(state)) {
					nextState = TERMINATED;
				}
				else {
					nextState = state | HAS_REQUEST;
				}

				if (STATE.compareAndSet(this, state, nextState)) {
					return state;
				}
			}
		}

		/**
		 * Sets current state to {@link #TERMINATED}
		 *
		 * @return previous state
		 */
		int markTerminated() {
			for (;;) {
				final int state = this.state;

				if (isTerminated(state)) {
					return TERMINATED;
				}

				if (STATE.compareAndSet(this, state, TERMINATED)) {
					return state;
				}
			}
		}

		/**
		 * Terminates execution if there is a demand from the downstream or sets
		 * {@link #HAS_VALUE} flag indicating that the delay process is completed
		 * however there is no demand from the downstream yet
		 */
		void complete() {
			for (; ; ) {
				int s = this.state;

				if (isTerminated(s)) {
					return;
				}

				if (hasRequest(s) && STATE.compareAndSet(this, s, TERMINATED)) {
					final CoreSubscriber<? super T> actual = this.actual;
					final T v = this.value;

					actual.onNext(v);
					actual.onComplete();

					return;
				}

				if (STATE.compareAndSet(this, s, s | HAS_VALUE)) {
					return;
				}
			}
		}
	}

	static final class DelayUntilTrigger<T> implements InnerConsumer<T> {

		final DelayUntilCoordinator<?> parent;

		Subscription s;
		boolean done;
		Throwable error;

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
			if (key == Attr.CANCELLED) return isTerminated(this.parent.state) && !this.done;
			if (key == Attr.PARENT) return this.s;
			if (key == Attr.ACTUAL) return this.parent;
			if (key == Attr.ERROR) return this.error;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				int previousState = markInnerActive();
				if (isTerminated(previousState)) {
					s.cancel();

					final DelayUntilCoordinator<?> parent = this.parent;
					Operators.onDiscard(parent.value, parent.currentContext());

					return;
				}

				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			//NO-OP
			Operators.onDiscard(t, this.parent.currentContext());
		}

		@Override
		public void onError(Throwable t) {
			if (this.done) {
				Operators.onErrorDropped(t, parent.currentContext());
				return;
			}

			final DelayUntilCoordinator<?> parent = this.parent;

			this.done = true;
			parent.done = true;

			if (!Exceptions.addThrowable(DelayUntilCoordinator.ERROR, parent, t)) {
				Operators.onErrorDropped(t, parent.currentContext());
				return;
			}

			final int previousState = parent.markTerminated();
			if (isTerminated(previousState)) {
				return;
			}

			Operators.onDiscard(parent.value, parent.currentContext());

			parent.s.cancel();

			final Throwable e = Exceptions.terminate(DelayUntilCoordinator.ERROR, parent);
			//noinspection ConstantConditions
			parent.actual.onError(e);
		}

		@Override
		public void onComplete() {
			if (this.done) {
				return;
			}

			this.done = true;

			final DelayUntilCoordinator<?> parent = this.parent;
			final int nextIndex = parent.index + 1;

			parent.index = nextIndex;

			if (nextIndex == parent.otherGenerators.length) {
				parent.complete();
				return;
			}

			final int previousState = markInnerInactive();
			if (isTerminated(previousState)) {
				return;
			}

			// we have to reset the state since we reuse the same object for the
			// optimization purpose and at this stage we know that there is another
			// delayer Publisher to subscribe to
			this.done = false;
			this.s = null;

			parent.subscribeNextTrigger();
		}

		void cancel() {
			this.s.cancel();
		}

		/**
		 * Sets flag {@link DelayUntilCoordinator#HAS_INNER} which indicates that there
		 * is an active delayer Publisher
		 *
		 * @return previous state
		 */
		int markInnerActive() {
			final DelayUntilCoordinator<?> parent = this.parent;
			for (;;) {
				final int state = parent.state;

				if (isTerminated(state)) {
					return DelayUntilCoordinator.TERMINATED;
				}

				if (hasInner(state)) {
					return state;
				}

				if (DelayUntilCoordinator.STATE.compareAndSet(parent, state, state | DelayUntilCoordinator.HAS_INNER)) {
					return state;
				}
			}
		}

		/**
		 * Unsets flag {@link DelayUntilCoordinator#HAS_INNER}
		 *
		 * @return previous state
		 */
		int markInnerInactive() {
			final DelayUntilCoordinator<?> parent = this.parent;
			for (;;) {
				final int state = parent.state;

				if (isTerminated(state)) {
					return DelayUntilCoordinator.TERMINATED;
				}

				if (!hasInner(state)) {
					return state;
				}

				if (DelayUntilCoordinator.STATE.compareAndSet(parent, state, state &~ DelayUntilCoordinator.HAS_INNER)) {
					return state;
				}
			}
		}
	}

	/**
	 * Indicates if state is ended with cancellation | error | complete
	 */
	static boolean isTerminated(int state) {
		return state == DelayUntilCoordinator.TERMINATED;
	}

	static boolean hasValue(int state) {
		return (state & DelayUntilCoordinator.HAS_VALUE) == DelayUntilCoordinator.HAS_VALUE;
	}

	static boolean hasInner(int state) {
		return (state & DelayUntilCoordinator.HAS_INNER) == DelayUntilCoordinator.HAS_INNER;
	}

	static boolean hasRequest(int state) {
		return (state & DelayUntilCoordinator.HAS_REQUEST) == DelayUntilCoordinator.HAS_REQUEST;
	}

	static boolean hasSubscription(int state) {
		return (state & DelayUntilCoordinator.HAS_SUBSCRIPTION) == DelayUntilCoordinator.HAS_SUBSCRIPTION;
	}
}
