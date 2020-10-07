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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * A Publisher that correlates two Publishers when they overlap in time and groups the
 * results.
 * <p>
 * There are no guarantees in what order the items get combined when multiple items from
 * one or both source Publishers overlap.
 *
 * @param <TLeft> the left Publisher to correlate items from the source Publisher with
 * @param <TRight> the other Publisher to correlate items from the source Publisher with
 * @param <TLeftEnd> type that a function returns via a Publisher whose emissions indicate
 * the duration of the values of the source Publisher
 * @param <TRightEnd> type that a function that returns via a Publisher whose emissions
 * indicate the duration of the values of the {@code right} Publisher
 * @param <R> type that a function that takes an item emitted by each Publisher and
 * returns the value to be emitted by the resulting Publisher
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 * @since 3.0
 */
final class FluxGroupJoin<TLeft, TRight, TLeftEnd, TRightEnd, R>
		extends InternalFluxOperator<TLeft, R> {

	final Publisher<? extends TRight> other;

	final Function<? super TLeft, ? extends Publisher<TLeftEnd>> leftEnd;

	final Function<? super TRight, ? extends Publisher<TRightEnd>> rightEnd;

	final BiFunction<? super TLeft, ? super Flux<TRight>, ? extends R> resultSelector;

	final Supplier<? extends Queue<TRight>> processorQueueSupplier;

	FluxGroupJoin(Flux<TLeft> source,
			Publisher<? extends TRight> other,
			Function<? super TLeft, ? extends Publisher<TLeftEnd>> leftEnd,
			Function<? super TRight, ? extends Publisher<TRightEnd>> rightEnd,
			BiFunction<? super TLeft, ? super Flux<TRight>, ? extends R> resultSelector,
			Supplier<? extends Queue<Object>> queueSupplier,
			Supplier<? extends Queue<TRight>> processorQueueSupplier) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
		this.leftEnd = Objects.requireNonNull(leftEnd, "leftEnd");
		this.rightEnd = Objects.requireNonNull(rightEnd, "rightEnd");
		this.processorQueueSupplier = Objects.requireNonNull(processorQueueSupplier, "processorQueueSupplier");
		this.resultSelector = Objects.requireNonNull(resultSelector, "resultSelector");
	}

	@Override
	public CoreSubscriber<? super TLeft> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		GroupJoinSubscription<TLeft, TRight, TLeftEnd, TRightEnd, R> parent =
				new GroupJoinSubscription<>(actual,
						leftEnd,
						rightEnd,
						resultSelector,
						processorQueueSupplier);

		actual.onSubscribe(parent);

		LeftRightSubscriber left = new LeftRightSubscriber(parent, true);
		parent.cancellations.add(left);
		LeftRightSubscriber right = new LeftRightSubscriber(parent, false);
		parent.cancellations.add(right);

		source.subscribe(left);
		other.subscribe(right);
		return null;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	interface JoinSupport<T> extends InnerProducer<T> {

		void innerError(Throwable ex);

		void innerComplete(LeftRightSubscriber sender);

		void innerValue(boolean isLeft, Object o);

		void innerClose(boolean isLeft, LeftRightEndSubscriber index);

		void innerCloseError(Throwable ex);
	}

	static final class GroupJoinSubscription<TLeft, TRight, TLeftEnd, TRightEnd, R>
			implements JoinSupport<R> {

		final Queue<Object>               queue;
		final BiPredicate<Object, Object> queueBiOffer;

		final Disposable.Composite cancellations;

		final Map<Integer, Sinks.Many<TRight>> lefts;

		final Map<Integer, TRight> rights;

		final Function<? super TLeft, ? extends Publisher<TLeftEnd>> leftEnd;

		final Function<? super TRight, ? extends Publisher<TRightEnd>> rightEnd;

		final BiFunction<? super TLeft, ? super Flux<TRight>, ? extends R> resultSelector;

		final Supplier<? extends Queue<TRight>> processorQueueSupplier;

		final CoreSubscriber<? super R>             actual;

		int leftIndex;

		int rightIndex;

		volatile int wip;

		static final AtomicIntegerFieldUpdater<GroupJoinSubscription> WIP =
				AtomicIntegerFieldUpdater.newUpdater(GroupJoinSubscription.class, "wip");

		volatile int active;

		static final AtomicIntegerFieldUpdater<GroupJoinSubscription> ACTIVE =
				AtomicIntegerFieldUpdater.newUpdater(GroupJoinSubscription.class,
						"active");

		volatile long requested;

		static final AtomicLongFieldUpdater<GroupJoinSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(GroupJoinSubscription.class,
						"requested");

		volatile Throwable error;

		static final AtomicReferenceFieldUpdater<GroupJoinSubscription, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(GroupJoinSubscription.class,
						Throwable.class,
						"error");

		static final Integer LEFT_VALUE = 1;

		static final Integer RIGHT_VALUE = 2;

		static final Integer LEFT_CLOSE = 3;

		static final Integer RIGHT_CLOSE = 4;

		@SuppressWarnings("unchecked")
		GroupJoinSubscription(CoreSubscriber<? super R> actual,
				Function<? super TLeft, ? extends Publisher<TLeftEnd>> leftEnd,
				Function<? super TRight, ? extends Publisher<TRightEnd>> rightEnd,
				BiFunction<? super TLeft, ? super Flux<TRight>, ? extends R> resultSelector,
				Supplier<? extends Queue<TRight>> processorQueueSupplier) {
			this.actual = actual;
			this.cancellations = Disposables.composite();
			this.processorQueueSupplier = processorQueueSupplier;
			this.queue = Queues.unboundedMultiproducer().get();
			this.queueBiOffer = (BiPredicate) queue;
			this.lefts = new LinkedHashMap<>();
			this.rights = new LinkedHashMap<>();
			this.leftEnd = leftEnd;
			this.rightEnd = rightEnd;
			this.resultSelector = resultSelector;
			ACTIVE.lazySet(this, 2);
		}

		@Override
		public final CoreSubscriber<? super R> actual() {
			return actual;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.concat(
					lefts.values().stream().map(Scannable::from),
					Scannable.from(cancellations).inners()
			);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.CANCELLED) return cancellations.isDisposed();
			if (key == Attr.BUFFERED) return queue.size() / 2;
			if (key == Attr.TERMINATED) return active == 0;
			if (key == Attr.ERROR) return error;

			return JoinSupport.super.scanUnsafe(key);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			if (cancellations.isDisposed()) {
				return;
			}
			cancellations.dispose();
			if (WIP.getAndIncrement(this) == 0) {
				queue.clear();
			}
		}

		void errorAll(Subscriber<?> a) {
			Throwable ex = Exceptions.terminate(ERROR, this);

			for (Sinks.Many<TRight> up : lefts.values()) {
				up.emitError(ex, Sinks.EmitFailureHandler.FAIL_FAST);
			}

			lefts.clear();
			rights.clear();

			a.onError(ex);
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;
			Queue<Object> q = queue;
			Subscriber<? super R> a = actual;

			for (; ; ) {
				for (; ; ) {
					if (cancellations.isDisposed()) {
						q.clear();
						return;
					}

					Throwable ex = error;
					if (ex != null) {
						q.clear();
						cancellations.dispose();
						errorAll(a);
						return;
					}

					boolean d = active == 0;

					Integer mode = (Integer) q.poll();

					boolean empty = mode == null;

					if (d && empty) {
						for (Sinks.Many<?> up : lefts.values()) {
							up.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
						}

						lefts.clear();
						rights.clear();
						cancellations.dispose();

						a.onComplete();
						return;
					}

					if (empty) {
						break;
					}

					Object val = q.poll();

					if (mode == LEFT_VALUE) {
						@SuppressWarnings("unchecked") TLeft left = (TLeft) val;

						Sinks.Many<TRight> up = Sinks.unsafe().many().unicast().onBackpressureBuffer(processorQueueSupplier.get());
						int idx = leftIndex++;
						lefts.put(idx, up);

						Publisher<TLeftEnd> p;

						try {
							p = Objects.requireNonNull(leftEnd.apply(left),
									"The leftEnd returned a null Publisher");
						}
						catch (Throwable exc) {
							Exceptions.addThrowable(ERROR,
									this,
									Operators.onOperatorError(this, exc, left,
											actual.currentContext()));
							errorAll(a);
							return;
						}

						LeftRightEndSubscriber end =
								new LeftRightEndSubscriber(this, true, idx);
						cancellations.add(end);

						p.subscribe(end);

						ex = error;
						if (ex != null) {
							cancellations.dispose();
							q.clear();
							errorAll(a);
							return;
						}

						R w;

						try {
							w = Objects.requireNonNull(resultSelector.apply(left, up.asFlux()),
									"The resultSelector returned a null value");
						}
						catch (Throwable exc) {
							Exceptions.addThrowable(ERROR,
									this, Operators.onOperatorError(this, exc, up,
											actual.currentContext()));
							errorAll(a);
							return;
						}

						// TODO since only left emission calls the actual, it is possible to link downstream backpressure with left's source and not error out
						long r = requested;
						if (r != 0L) {
							a.onNext(w);
							Operators.produced(REQUESTED, this, 1);
						}
						else {
							Exceptions.addThrowable(ERROR,
									this,
									Exceptions.failWithOverflow());
							errorAll(a);
							return;
						}

						for (TRight right : rights.values()) {
							up.emitNext(right, Sinks.EmitFailureHandler.FAIL_FAST);
						}
					}
					else if (mode == RIGHT_VALUE) {
						@SuppressWarnings("unchecked") TRight right = (TRight) val;

						int idx = rightIndex++;

						rights.put(idx, right);

						Publisher<TRightEnd> p;

						try {
							p = Objects.requireNonNull(rightEnd.apply(right),
									"The rightEnd returned a null Publisher");
						}
						catch (Throwable exc) {
							Exceptions.addThrowable(ERROR,
									this,
									Operators.onOperatorError(this, exc, right,
											actual.currentContext()));
							errorAll(a);
							return;
						}

						LeftRightEndSubscriber end =
								new LeftRightEndSubscriber(this, false, idx);
						cancellations.add(end);

						p.subscribe(end);

						ex = error;
						if (ex != null) {
							q.clear();
							cancellations.dispose();
							errorAll(a);
							return;
						}

						for (Sinks.Many<TRight> up : lefts.values()) {
							up.emitNext(right, Sinks.EmitFailureHandler.FAIL_FAST);
						}
					}
					else if (mode == LEFT_CLOSE) {
						LeftRightEndSubscriber end = (LeftRightEndSubscriber) val;

						Sinks.Many<TRight> up = lefts.remove(end.index);
						cancellations.remove(end);
						if (up != null) {
							up.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
						}
					}
					else if (mode == RIGHT_CLOSE) {
						LeftRightEndSubscriber end = (LeftRightEndSubscriber) val;

						rights.remove(end.index);
						cancellations.remove(end);
					}
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		public void innerError(Throwable ex) {
			if (Exceptions.addThrowable(ERROR, this, ex)) {
				ACTIVE.decrementAndGet(this);
				drain();
			}
			else {
				Operators.onErrorDropped(ex, actual.currentContext());
			}
		}

		@Override
		public void innerComplete(LeftRightSubscriber sender) {
			cancellations.remove(sender);
			ACTIVE.decrementAndGet(this);
			drain();
		}

		@Override
		public void innerValue(boolean isLeft, Object o) {
			queueBiOffer.test(isLeft ? LEFT_VALUE : RIGHT_VALUE, o);
			drain();
		}

		@Override
		public void innerClose(boolean isLeft, LeftRightEndSubscriber index) {
			queueBiOffer.test(isLeft ? LEFT_CLOSE : RIGHT_CLOSE, index);
			drain();
		}

		@Override
		public void innerCloseError(Throwable ex) {
			if (Exceptions.addThrowable(ERROR, this, ex)) {
				drain();
			}
			else {
				Operators.onErrorDropped(ex, actual.currentContext());
			}
		}
	}

	static final class LeftRightSubscriber
			implements InnerConsumer<Object>, Disposable {

		final JoinSupport<?> parent;

		final boolean isLeft;

		volatile Subscription subscription;

		final static AtomicReferenceFieldUpdater<LeftRightSubscriber, Subscription>
				SUBSCRIPTION =
				AtomicReferenceFieldUpdater.newUpdater(LeftRightSubscriber.class,
						Subscription.class,
						"subscription");

		LeftRightSubscriber(JoinSupport<?> parent, boolean isLeft) {
			this.parent = parent;
			this.isLeft = isLeft;
		}

		@Override
		public void dispose() {
			Operators.terminate(SUBSCRIPTION, this);
		}

		@Override
		public Context currentContext() {
			return parent.actual().currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return subscription;
			if (key == Attr.ACTUAL ) return parent;
			if (key == Attr.CANCELLED) return isDisposed();
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}

		@Override
		public boolean isDisposed() {
			return Operators.cancelledSubscription() == subscription;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(SUBSCRIPTION, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(Object t) {
			parent.innerValue(isLeft, t);
		}

		@Override
		public void onError(Throwable t) {
			parent.innerError(t);
		}

		@Override
		public void onComplete() {
			parent.innerComplete(this);
		}

	}

	static final class LeftRightEndSubscriber
			implements InnerConsumer<Object>, Disposable {

		final JoinSupport<?> parent;

		final boolean isLeft;

		final int index;

		volatile Subscription subscription;

		final static AtomicReferenceFieldUpdater<LeftRightEndSubscriber, Subscription>
				SUBSCRIPTION = AtomicReferenceFieldUpdater.newUpdater(
				LeftRightEndSubscriber.class,
				Subscription.class,
				"subscription");

		LeftRightEndSubscriber(JoinSupport<?> parent, boolean isLeft, int index) {
			this.parent = parent;
			this.isLeft = isLeft;
			this.index = index;
		}

		@Override
		public void dispose() {
			Operators.terminate(SUBSCRIPTION, this);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return subscription;
			if (key == Attr.CANCELLED) return isDisposed();
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}

		@Override
		public boolean isDisposed() {
			return Operators.cancelledSubscription() == subscription;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(SUBSCRIPTION, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(Object t) {
			if (Operators.terminate(SUBSCRIPTION, this)) {
				parent.innerClose(isLeft, this);
			}
		}

		@Override
		public void onError(Throwable t) {
			parent.innerError(t);
		}

		@Override
		public void onComplete() {
			parent.innerClose(isLeft, this);
		}

		@Override
		public Context currentContext() {
			return parent.actual().currentContext();
		}

	}
}
