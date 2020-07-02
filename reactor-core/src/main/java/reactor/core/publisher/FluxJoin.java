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
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.FluxGroupJoin.JoinSupport;
import reactor.core.publisher.FluxGroupJoin.LeftRightEndSubscriber;
import reactor.core.publisher.FluxGroupJoin.LeftRightSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 * @since 3.0
 */
final class FluxJoin<TLeft, TRight, TLeftEnd, TRightEnd, R> extends
		InternalFluxOperator<TLeft, R> {

	final Publisher<? extends TRight> other;

	final Function<? super TLeft, ? extends Publisher<TLeftEnd>> leftEnd;

	final Function<? super TRight, ? extends Publisher<TRightEnd>> rightEnd;

	final BiFunction<? super TLeft, ? super TRight, ? extends R> resultSelector;

	FluxJoin(Flux<TLeft> source,
			Publisher<? extends TRight> other,
			Function<? super TLeft, ? extends Publisher<TLeftEnd>> leftEnd,
			Function<? super TRight, ? extends Publisher<TRightEnd>> rightEnd,
			BiFunction<? super TLeft, ? super TRight, ? extends R> resultSelector) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
		this.leftEnd = Objects.requireNonNull(leftEnd, "leftEnd");
		this.rightEnd = Objects.requireNonNull(rightEnd, "rightEnd");
		this.resultSelector = Objects.requireNonNull(resultSelector, "resultSelector");
	}

	@Override
	public CoreSubscriber<? super TLeft> subscribeOrReturn(CoreSubscriber<? super R> actual) {

		JoinSubscription<TLeft, TRight, TLeftEnd, TRightEnd, R> parent =
				new JoinSubscription<>(actual,
						leftEnd,
						rightEnd,
						resultSelector);

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

	static final class JoinSubscription<TLeft, TRight, TLeftEnd, TRightEnd, R>
			implements JoinSupport<R> {

		final Queue<Object>               queue;
		final BiPredicate<Object, Object> queueBiOffer;

		final Disposable.Composite cancellations;

		final Map<Integer, TLeft> lefts;

		final Map<Integer, TRight> rights;

		final Function<? super TLeft, ? extends Publisher<TLeftEnd>> leftEnd;

		final Function<? super TRight, ? extends Publisher<TRightEnd>> rightEnd;

		final BiFunction<? super TLeft, ? super TRight, ? extends R> resultSelector;
		final CoreSubscriber<? super R>                                  actual;

		volatile int wip;

		static final AtomicIntegerFieldUpdater<JoinSubscription> WIP =
				AtomicIntegerFieldUpdater.newUpdater(JoinSubscription.class, "wip");

		volatile int active;

		static final AtomicIntegerFieldUpdater<JoinSubscription> ACTIVE =
				AtomicIntegerFieldUpdater.newUpdater(JoinSubscription.class,
						"active");

		volatile long requested;

		static final AtomicLongFieldUpdater<JoinSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(JoinSubscription.class,
						"requested");

		volatile Throwable error;

		static final AtomicReferenceFieldUpdater<JoinSubscription, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(JoinSubscription.class,
						Throwable.class,
						"error");

		int leftIndex;

		int rightIndex;

		static final Integer LEFT_VALUE = 1;

		static final Integer RIGHT_VALUE = 2;

		static final Integer LEFT_CLOSE = 3;

		static final Integer RIGHT_CLOSE = 4;

		@SuppressWarnings("unchecked")
		JoinSubscription(CoreSubscriber<? super R> actual,
				Function<? super TLeft, ? extends Publisher<TLeftEnd>> leftEnd,
				Function<? super TRight, ? extends Publisher<TRightEnd>> rightEnd,
				BiFunction<? super TLeft, ? super TRight, ? extends R> resultSelector) {
			this.actual = actual;
			this.cancellations = Disposables.composite();
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
			return Scannable.from(cancellations).inners();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.CANCELLED) return cancellations.isDisposed();
			if (key == Attr.BUFFERED) return queue.size() / 2;
			if (key == Attr.TERMINATED) return active == 0;
			if (key == Attr.ERROR) return error;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

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

						int idx = leftIndex++;
						lefts.put(idx, left);

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
							q.clear();
							cancellations.dispose();
							errorAll(a);
							return;
						}

						long r = requested;
						long e = 0L;

						for (TRight right : rights.values()) {

							R w;

							try {
								w = Objects.requireNonNull(resultSelector.apply(left,
										right),
										"The resultSelector returned a null value");
							}
							catch (Throwable exc) {
								Exceptions.addThrowable(ERROR,
										this,
										Operators.onOperatorError(this,
												exc, right, actual.currentContext()));
								errorAll(a);
								return;
							}

							if (e != r) {
								a.onNext(w);

								e++;
							}
							else {
								Exceptions.addThrowable(ERROR,
										this,
										Exceptions.failWithOverflow("Could not " + "emit value due to lack of requests"));
								q.clear();
								cancellations.dispose();
								errorAll(a);
								return;
							}
						}

						if (e != 0L) {
							Operators.produced(REQUESTED, this, e);
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

						long r = requested;
						long e = 0L;

						for (TLeft left : lefts.values()) {

							R w;

							try {
								w = Objects.requireNonNull(resultSelector.apply(left,
										right),
										"The resultSelector returned a null value");
							}
							catch (Throwable exc) {
								Exceptions.addThrowable(ERROR,
										this,
										Operators.onOperatorError(this, exc, left,
												actual.currentContext()));
								errorAll(a);
								return;
							}

							if (e != r) {
								a.onNext(w);

								e++;
							}
							else {
								Exceptions.addThrowable(ERROR,
										this,
										Exceptions.failWithOverflow("Could not emit " + "value due to lack of requests"));
								q.clear();
								cancellations.dispose();
								errorAll(a);
								return;
							}
						}

						if (e != 0L) {
							Operators.produced(REQUESTED, this, e);
						}
					}
					else if (mode == LEFT_CLOSE) {
						LeftRightEndSubscriber end = (LeftRightEndSubscriber) val;

						lefts.remove(end.index);
						cancellations.remove(end);
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
}
