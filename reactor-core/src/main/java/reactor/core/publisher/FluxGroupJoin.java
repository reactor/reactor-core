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
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.concurrent.OpenHashSet;
import reactor.util.context.Context;
import reactor.util.context.ContextRelay;
import javax.annotation.Nullable;

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
		extends FluxOperator<TLeft, R> {

	final Publisher<? extends TRight> other;

	final Function<? super TLeft, ? extends Publisher<TLeftEnd>> leftEnd;

	final Function<? super TRight, ? extends Publisher<TRightEnd>> rightEnd;

	final BiFunction<? super TLeft, ? super Flux<TRight>, ? extends R> resultSelector;

	final Supplier<? extends Queue<Object>> queueSupplier;
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
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
		this.processorQueueSupplier =
				Objects.requireNonNull(processorQueueSupplier, "processorQueueSupplier");
		this.resultSelector = Objects.requireNonNull(resultSelector, "resultSelector");
	}

	@Override
	public void subscribe(Subscriber<? super R> s, Context ctx) {

		GroupJoinSubscription<TLeft, TRight, TLeftEnd, TRightEnd, R> parent =
				new GroupJoinSubscription<>(s,
						leftEnd,
						rightEnd,
						resultSelector,
						queueSupplier.get(),
						processorQueueSupplier);

		s.onSubscribe(parent);

		LeftRightSubscriber left = new LeftRightSubscriber(parent, true);
		parent.cancellations.add(left);
		LeftRightSubscriber right = new LeftRightSubscriber(parent, false);
		parent.cancellations.add(right);

		source.subscribe(left, ctx);
		other.subscribe(right);
	}

	interface JoinSupport extends ContextRelay {

		void innerError(Throwable ex);

		void innerComplete(LeftRightSubscriber sender);

		void innerValue(boolean isLeft, Object o);

		void innerClose(boolean isLeft, LeftRightEndSubscriber index);

		void innerCloseError(Throwable ex);
	}

	static final class GroupJoinSubscription<TLeft, TRight, TLeftEnd, TRightEnd, R>
			extends CachedContextProducer<R>
			implements JoinSupport {

		final Queue<Object>               queue;
		final BiPredicate<Object, Object> queueBiOffer;

		final OpenHashSet<Disposable> cancellations;

		final Map<Integer, UnicastProcessor<TRight>> lefts;

		final Map<Integer, TRight> rights;

		final Function<? super TLeft, ? extends Publisher<TLeftEnd>> leftEnd;

		final Function<? super TRight, ? extends Publisher<TRightEnd>> rightEnd;

		final BiFunction<? super TLeft, ? super Flux<TRight>, ? extends R> resultSelector;

		final Supplier<? extends Queue<TRight>> processorQueueSupplier;

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

		volatile boolean cancelled;

		static final Integer LEFT_VALUE = 1;

		static final Integer RIGHT_VALUE = 2;

		static final Integer LEFT_CLOSE = 3;

		static final Integer RIGHT_CLOSE = 4;

		@SuppressWarnings("unchecked")
		GroupJoinSubscription(Subscriber<? super R> actual,
				Function<? super TLeft, ? extends Publisher<TLeftEnd>> leftEnd,
				Function<? super TRight, ? extends Publisher<TRightEnd>> rightEnd,
				BiFunction<? super TLeft, ? super Flux<TRight>, ? extends R> resultSelector,
				Queue<Object> queue,
				Supplier<? extends
						Queue<TRight>> processorQueueSupplier) {
			super(actual);
			this.cancellations = new OpenHashSet<>();
			this.queue = queue;
			this.processorQueueSupplier = processorQueueSupplier;
			if (!(queue instanceof BiPredicate)) {
				throw new IllegalArgumentException("The provided queue must implement " + "BiPredicate to expose atomic dual insert");
			}
			this.queueBiOffer = (BiPredicate) queue;
			this.lefts = new LinkedHashMap<>();
			this.rights = new LinkedHashMap<>();
			this.leftEnd = leftEnd;
			this.rightEnd = rightEnd;
			this.resultSelector = resultSelector;
			ACTIVE.lazySet(this, 2);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.concat(
					lefts.values().stream(),
					Stream.of(cancellations.keys()).map(Scannable::from)
			);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == BooleanAttr.CANCELLED) return cancelled;
			if (key == IntAttr.BUFFERED) return queue.size() / 2;
			if (key == BooleanAttr.TERMINATED) return active == 0;
			if (key == ThrowableAttr.ERROR) return error;

			return super.scanUnsafe(key);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}
			cancelled = true;
			cancelAll();
			if (WIP.getAndIncrement(this) == 0) {
				queue.clear();
			}
		}

		void cancelAll() {
			Object[] a = cancellations.keys();
			for (Object o : a) {
				if (o != null) {
					((Disposable) o).dispose();
				}
			}
		}

		void errorAll(Subscriber<?> a) {
			Throwable ex = Exceptions.terminate(ERROR, this);

			for (UnicastProcessor<TRight> up : lefts.values()) {
				up.onError(ex);
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
					if (cancelled) {
						q.clear();
						return;
					}

					Throwable ex = error;
					if (ex != null) {
						q.clear();
						cancelAll();
						errorAll(a);
						return;
					}

					boolean d = active == 0;

					Integer mode = (Integer) q.poll();

					boolean empty = mode == null;

					if (d && empty) {
						for (UnicastProcessor<?> up : lefts.values()) {
							up.onComplete();
						}

						lefts.clear();
						rights.clear();
						cancelAll();

						a.onComplete();
						return;
					}

					if (empty) {
						break;
					}

					Object val = q.poll();

					if (mode == LEFT_VALUE) {
						@SuppressWarnings("unchecked") TLeft left = (TLeft) val;

						UnicastProcessor<TRight> up =
								new UnicastProcessor<>(processorQueueSupplier.get());
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
									Operators.onOperatorError(this, exc, left));
							errorAll(a);
							return;
						}

						LeftRightEndSubscriber end =
								new LeftRightEndSubscriber(this, true, idx);
						cancellations.add(end);

						p.subscribe(end);

						ex = error;
						if (ex != null) {
							cancelAll();
							q.clear();
							errorAll(a);
							return;
						}

						R w;

						try {
							w = Objects.requireNonNull(resultSelector.apply(left, up),
									"The resultSelector returned a null value");
						}
						catch (Throwable exc) {
							Exceptions.addThrowable(ERROR,
									this, Operators.onOperatorError(this, exc, up));
							errorAll(a);
							return;
						}

						// TODO since only left emission calls the actual, it is possible to link downstream backpressure with left's source and not error out
						long r = requested;
						if (r != 0L) {
							a.onNext(w);
							long upd;
							for (; ; ) {
								if (r == Long.MAX_VALUE) {
									break;
								}
								upd = r - 1L;
								if (upd < 0L) {
									Exceptions.addThrowable(ERROR,
											this,
											Operators.onOperatorError(this,
													Exceptions.failWithOverflow()));
									errorAll(a);
									return;
								}
								if (REQUESTED.compareAndSet(this, r, upd)) {
									break;
								}
								r = requested;
							}
						}
						else {
							Exceptions.addThrowable(ERROR,
									this,
									Exceptions.failWithOverflow());
							errorAll(a);
							return;
						}

						for (TRight right : rights.values()) {
							up.onNext(right);
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
									Operators.onOperatorError(this, exc, right));
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
							cancelAll();
							errorAll(a);
							return;
						}

						for (UnicastProcessor<TRight> up : lefts.values()) {
							up.onNext(right);
						}
					}
					else if (mode == LEFT_CLOSE) {
						LeftRightEndSubscriber end = (LeftRightEndSubscriber) val;

						UnicastProcessor<TRight> up = lefts.remove(end.index);
						cancellations.remove(end);
						if (up != null) {
							up.onComplete();
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
				Operators.onErrorDropped(ex);
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
			synchronized (this) {
				queueBiOffer.test(isLeft ? LEFT_VALUE : RIGHT_VALUE, o);
			}
			drain();
		}

		@Override
		public void innerClose(boolean isLeft, LeftRightEndSubscriber index) {
			synchronized (this) {
				queueBiOffer.test(isLeft ? LEFT_CLOSE : RIGHT_CLOSE, index);
			}
			drain();
		}

		@Override
		public void innerCloseError(Throwable ex) {
			if (Exceptions.addThrowable(ERROR, this, ex)) {
				drain();
			}
			else {
				Operators.onErrorDropped(ex);
			}
		}
	}

	static final class LeftRightSubscriber
			implements InnerConsumer<Object>, Disposable {

		final JoinSupport parent;

		final boolean isLeft;

		volatile Subscription subscription;

		final static AtomicReferenceFieldUpdater<LeftRightSubscriber, Subscription>
				SUBSCRIPTION =
				AtomicReferenceFieldUpdater.newUpdater(LeftRightSubscriber.class,
						Subscription.class,
						"subscription");

		LeftRightSubscriber(JoinSupport parent, boolean isLeft) {
			this.parent = parent;
			this.isLeft = isLeft;
		}

		@Override
		public void dispose() {
			Subscription current = SUBSCRIPTION.get(this);
			if (current != Operators.cancelledSubscription()) {
				current = SUBSCRIPTION.getAndSet(this, Operators.cancelledSubscription());
				if (current != Operators.cancelledSubscription()) {
					if (current != null) {
						current.cancel();
					}
				}
			}
		}

		@Override
		public void onContext(Context context) {
			if(isLeft){
				parent.onContext(context);
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return subscription;
			if (key == ScannableAttr.ACTUAL ) return parent;
			if (key == BooleanAttr.CANCELLED) return isDisposed();

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

		final JoinSupport parent;

		final boolean isLeft;

		final int index;

		volatile Subscription subscription;

		final static AtomicReferenceFieldUpdater<LeftRightEndSubscriber, Subscription>
				SUBSCRIPTION = AtomicReferenceFieldUpdater.newUpdater(
				LeftRightEndSubscriber.class,
				Subscription.class,
				"subscription");

		LeftRightEndSubscriber(JoinSupport parent, boolean isLeft, int index) {
			this.parent = parent;
			this.isLeft = isLeft;
			this.index = index;
		}

		@Override
		public void dispose() {
			Subscription current = SUBSCRIPTION.get(this);
			if (current != Operators.cancelledSubscription()) {
				current = SUBSCRIPTION.getAndSet(this, Operators.cancelledSubscription());
				if (current != Operators.cancelledSubscription()) {
					if (current != null) {
						current.cancel();
					}
				}
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return subscription;
			if (key == BooleanAttr.CANCELLED) return isDisposed();

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
			Subscription current = SUBSCRIPTION.get(this);
			if (current != Operators.cancelledSubscription()) {
				current = SUBSCRIPTION.getAndSet(this, Operators.cancelledSubscription());
				if (current != Operators.cancelledSubscription()) {
					if (current != null) {
						current.cancel();
					}
					parent.innerClose(isLeft, this);
				}
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
			return parent.currentContext();
		}

	}
}
