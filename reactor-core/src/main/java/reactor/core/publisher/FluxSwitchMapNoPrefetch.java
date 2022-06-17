/*
 * Copyright (c) 2021-2022 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Switches to a new Publisher generated via a function whenever the upstream produces an
 * item.
 *
 * @param <T> the source value type
 * @param <R> the output value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSwitchMapNoPrefetch<T, R> extends InternalFluxOperator<T, R> {

	final Function<? super T, ? extends Publisher<? extends R>> mapper;

	FluxSwitchMapNoPrefetch(Flux<? extends T> source,
			Function<? super T, ? extends Publisher<? extends R>> mapper) {
		super(source);
		this.mapper = Objects.requireNonNull(mapper, "mapper");
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		//for now switchMap doesn't support onErrorContinue, so the scalar version shouldn't either
		if (FluxFlatMap.trySubscribeScalarMap(source, actual, mapper, false, false)) {
			return null;
		}

		return new SwitchMapMain<T, R>(actual, mapper);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class SwitchMapMain<T, R> implements InnerOperator<T, R> {

		@Nullable
		final StateLogger logger;

		final Function<? super T, ? extends Publisher<? extends R>> mapper;
		final CoreSubscriber<? super R> actual;

		Subscription s;

		boolean done;

		SwitchMapInner<T, R> inner;

		volatile Throwable throwable;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SwitchMapMain, Throwable> THROWABLE =
				AtomicReferenceFieldUpdater.newUpdater(SwitchMapMain.class, Throwable.class, "throwable");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<SwitchMapMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(SwitchMapMain.class, "requested");

		volatile long state;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<SwitchMapMain> STATE =
				AtomicLongFieldUpdater.newUpdater(SwitchMapMain.class, "state");

		SwitchMapMain(CoreSubscriber<? super R> actual,
				Function<? super T, ? extends Publisher<? extends R>> mapper) {
			this(actual, mapper, null);
		}

		SwitchMapMain(CoreSubscriber<? super R> actual,
				Function<? super T, ? extends Publisher<? extends R>> mapper,
				@Nullable StateLogger logger) {
			this.actual = actual;
			this.mapper = mapper;
			this.logger = logger;
		}

		@Override
		public final CoreSubscriber<? super R> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			final long state = this.state;
			if (key == Attr.CANCELLED) return !this.done && state == TERMINATED;
			if (key == Attr.PARENT) return this.s;
			if (key == Attr.TERMINATED) return this.done;
			if (key == Attr.ERROR) return this.throwable;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return this.requested;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(this.inner);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				this.actual.onSubscribe(this);

				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (this.done) {
				Operators.onNextDropped(t, this.actual.currentContext());
				return;
			}

			final SwitchMapInner<T, R> si = this.inner;
			final boolean hasInner = si != null;

			if (!hasInner) {
				final SwitchMapInner<T, R> nsi = new SwitchMapInner<>(this, this.actual, 0, this.logger);
				this.inner = nsi;
				subscribeInner(t, nsi, 0);
				return;
			}

			final int nextIndex = si.index + 1;
			final SwitchMapInner<T, R> nsi = new SwitchMapInner<>(this, this.actual, nextIndex, this.logger);

			this.inner = nsi;
			si.nextInner = nsi;
			si.nextElement = t;

			long state = incrementIndex(this);
			if (state == TERMINATED) {
				Operators.onDiscard(t, this.actual.currentContext());
				return;
			}

			if (isInnerSubscribed(state)) {
				si.cancelFromParent();

				if (!isWip(state)) {
					final long produced = si.produced;
					if (produced > 0) {
						si.produced = 0;
						if (this.requested != Long.MAX_VALUE) {
							si.requested = 0;
							REQUESTED.addAndGet(this, -produced);
						}
					}
					subscribeInner(t, nsi, nextIndex);
				}
			}
		}

		void subscribeInner(T nextElement, SwitchMapInner<T, R> nextInner, int nextIndex) {
			final CoreSubscriber<? super R> actual = this.actual;
			final Context context = actual.currentContext();

			while (nextInner.index != nextIndex) {
				Operators.onDiscard(nextElement, context);
				nextElement = nextInner.nextElement;
				nextInner = nextInner.nextInner;
			}

			Publisher<? extends R> p;
			try {
				p = Objects.requireNonNull(this.mapper.apply(nextElement),
						"The mapper returned a null publisher");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(this.s, e, nextElement, context));
				return;
			}

			p.subscribe(nextInner);
		}

		@Override
		public void onError(Throwable t) {
			if (this.done) {
				Operators.onErrorDropped(t, this.actual.currentContext());
				return;
			}

			this.done = true;

			if (!Exceptions.addThrowable(THROWABLE, this, t)) {
				Operators.onErrorDropped(t, this.actual.currentContext());
				return;
			}

			final long state = setTerminated(this);
			if (state == TERMINATED) {
				return;
			}

			final SwitchMapInner<T, R> inner = this.inner;
			if (inner != null && isInnerSubscribed(state)) {
				inner.cancelFromParent();
			}

			//noinspection ConstantConditions
			this.actual.onError(Exceptions.terminate(THROWABLE, this));
		}

		@Override
		public void onComplete() {
			if (this.done) {
				return;
			}

			this.done = true;

			final long state = setMainCompleted(this);
			if (state == TERMINATED) {
				return;
			}

			final SwitchMapInner<T, R> inner = this.inner;
			if (inner == null || hasInnerCompleted(state)) {
				this.actual.onComplete();
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				long previousRequested = Operators.addCap(REQUESTED, this, n);
				long state = addRequest(this, previousRequested);

				if (state == TERMINATED) {
					return;
				}

				if (hasRequest(state) == 1 && isInnerSubscribed(state) && !hasInnerCompleted(state)) {
					final SwitchMapInner<T, R> inner = this.inner;
					if (inner.index == index(state)) {
						inner.request(n);
					}
				}
			}
		}

		@Override
		public void cancel() {
			final long state = setTerminated(this);
			if (state == TERMINATED) {
				return;
			}

			final SwitchMapInner<T, R> inner = this.inner;
			if (inner != null && isInnerSubscribed(state) && !hasInnerCompleted(state) && inner.index == index(state)) {
				inner.cancelFromParent();
			}

			if (!hasMainCompleted(state)) {
				this.s.cancel();

				//ensure that onError which races with cancel has its error dropped
				final Throwable e = Exceptions.terminate(THROWABLE, this);
				if (e != null) {
					Operators.onErrorDropped(e, this.actual.currentContext());
				}
			}
		}
	}

	static final class SwitchMapInner<T, R> implements InnerConsumer<R> {

		@Nullable
		final StateLogger logger;

		final SwitchMapMain<T, R> parent;
		final CoreSubscriber<? super R> actual;

		final int index;

		Subscription s;
		long         produced;
		long         requested;
		boolean      done;

		T nextElement;
		SwitchMapInner<T, R> nextInner;

		SwitchMapInner(SwitchMapMain<T, R> parent, CoreSubscriber<? super R> actual, int index, @Nullable StateLogger logger) {
			this.parent = parent;
			this.actual = actual;
			this.index = index;
			this.logger = logger;
		}

		@Override
		public Context currentContext() {
			return parent.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return isCancelledByParent();
			if (key == Attr.PARENT) return this.parent;
			if (key == Attr.ACTUAL) return this.actual;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				final int expectedIndex = this.index;
				final SwitchMapMain<T, R> parent = this.parent;
				final long state = setInnerSubscribed(parent, expectedIndex);

				if (state == TERMINATED) {
					s.cancel();
					return;
				}

				final int actualIndex = index(state);
				if (expectedIndex != actualIndex) {
					s.cancel();
					parent.subscribeInner(this.nextElement, this.nextInner, actualIndex);
					return;
				}

				if (hasRequest(state) > 0) {
					long requested = parent.requested;
					this.requested = requested;

					s.request(requested);
				}
			}
		}

		@Override
		public void onNext(R t) {
			if (this.done) {
				Operators.onNextDropped(t, this.actual.currentContext());
				return;
			}

			final SwitchMapMain<T, R> parent = this.parent;
			final Subscription s = this.s;
			final int expectedIndex = this.index;

			long requested = this.requested;
			long state = setWip(parent, expectedIndex);

			if (state == TERMINATED) {
				Operators.onDiscard(t, this.actual.currentContext());
				return;
			}

			int actualIndex = index(state);
			if (actualIndex != expectedIndex) {
				Operators.onDiscard(t, this.actual.currentContext());
				return;
			}

			this.actual.onNext(t);

			long produced = 0;
			boolean isDemandFulfilled = false;
			int expectedHasRequest = hasRequest(state);

			if (requested != Long.MAX_VALUE) {
				produced = this.produced + 1;
				this.produced = produced;

				if (expectedHasRequest > 1) {
					long actualRequested = parent.requested;
					long toRequestInAddition = actualRequested - requested;
					if (toRequestInAddition > 0) {
						requested = actualRequested;
						this.requested = requested;

						if (requested == Long.MAX_VALUE) {
							// we need to reset stats if unbounded requested
							this.produced = produced = 0;
							s.request(Long.MAX_VALUE);
						}
						else {
							s.request(toRequestInAddition);
						}
					}
				}

				isDemandFulfilled = produced == requested;
				if (isDemandFulfilled) {
					this.produced = 0;
					requested = SwitchMapMain.REQUESTED.addAndGet(parent, -produced);
					this.requested = requested;

					produced = 0;
					isDemandFulfilled = requested == 0;
					if (!isDemandFulfilled) {
						s.request(requested);
					}
				}
			}

			for (;;) {
				state = unsetWip(parent, expectedIndex, isDemandFulfilled, expectedHasRequest);

				if (state == TERMINATED) {
					return;
				}

				actualIndex = index(state);
				if (expectedIndex != actualIndex) {
					if (produced > 0) {
						this.produced = 0;
						this.requested = 0;

						SwitchMapMain.REQUESTED.addAndGet(parent, -produced);
					}
					parent.subscribeInner(this.nextElement, this.nextInner, actualIndex);
					return;
				}

				int actualHasRequest = hasRequest(state);
				if (isDemandFulfilled && expectedHasRequest < actualHasRequest) {
					expectedHasRequest = actualHasRequest;

					long currentRequest = parent.requested;
					long toRequestInAddition = currentRequest - requested;
					if (toRequestInAddition > 0) {
						requested = currentRequest;
						this.requested = requested;

						isDemandFulfilled = false;
						s.request(requested == Long.MAX_VALUE ? Long.MAX_VALUE : toRequestInAddition);
					}

					continue;
				}

				return;
			}
		}

		@Override
		public void onError(Throwable t) {
			if (this.done) {
				Operators.onErrorDropped(t, this.actual.currentContext());
				return;
			}

			this.done = true;

			final SwitchMapMain<T, R> parent = this.parent;

			// ensures that racing on setting error will pass smoothly
			if (!Exceptions.addThrowable(SwitchMapMain.THROWABLE, parent, t)) {
				Operators.onErrorDropped(t, this.actual.currentContext());
				return;
			}

			final long state = setTerminated(parent);
			if (state == TERMINATED) {
				return;
			}

			if (!hasMainCompleted(state)) {
				parent.s.cancel();
			}

			//noinspection ConstantConditions
			this.actual.onError(Exceptions.terminate(SwitchMapMain.THROWABLE, parent));
		}

		@Override
		public void onComplete() {
			if (this.done) {
				return;
			}

			this.done = true;

			final SwitchMapMain<T, R> parent = this.parent;
			final int expectedIndex = this.index;

			long state = setWip(parent, expectedIndex);
			if (state == TERMINATED) {
				return;
			}

			int actualIndex = index(state);
			if (actualIndex != expectedIndex) {
				return;
			}

			final long produced = this.produced;
			if (produced > 0) {
				this.produced = 0;
				this.requested = 0;
				SwitchMapMain.REQUESTED.addAndGet(parent, -produced);
			}

			if (hasMainCompleted(state)) {
				this.actual.onComplete();
				return;
			}

			state = setInnerCompleted(parent);
			if (state == TERMINATED) {
				return;
			}

			actualIndex = index(state);
			if (expectedIndex != actualIndex) {
				parent.subscribeInner(this.nextElement, this.nextInner, actualIndex);
			} else if (hasMainCompleted(state)) {
				this.actual.onComplete();
			}
		}

		void request(long n) {
			long requested = this.requested;
			this.requested = Operators.addCap(requested, n);

			this.s.request(n);
		}

		boolean isCancelledByParent() {
			long state = parent.state;
			return (this.index != index(state) && !this.done) || (!parent.done && state == TERMINATED);
		}

		void cancelFromParent() {
			this.s.cancel();
		}

		@Override
		public String toString() {
			return new StringJoiner(", ",
					SwitchMapInner.class.getSimpleName() + "[",
					"]").add("index=" + index)
			            .toString();
		}
	}

	static int  INDEX_OFFSET          = 32;
	static int  HAS_REQUEST_OFFSET    = 4;
	static long TERMINATED            =
			0b11111111111111111111111111111111_1111111111111111111111111111_1_1_1_1L;
	static long INNER_WIP_MASK        =
			0b00000000000000000000000000000000_0000000000000000000000000000_0_0_0_1L;
	static long INNER_SUBSCRIBED_MASK =
			0b00000000000000000000000000000000_0000000000000000000000000000_0_0_1_0L;
	static long INNER_COMPLETED_MASK  =
			0b00000000000000000000000000000000_0000000000000000000000000000_0_1_0_0L;
	static long COMPLETED_MASK        =
			0b00000000000000000000000000000000_0000000000000000000000000000_1_0_0_0L;
	static long HAS_REQUEST_MASK      =
			0b00000000000000000000000000000000_1111111111111111111111111111_0_0_0_0L;
	static int  MAX_HAS_REQUEST       = 0b0000_1111111111111111111111111111;

	/**
	 * Atomically set state as terminated
	 *
	 * @return previous state
	 */
	static long setTerminated(SwitchMapMain<?, ?> instance) {
		for (;;) {
			final long state = instance.state;

			// do nothing if stream is terminated
			if (state == TERMINATED) {
				return TERMINATED;
			}

			if (SwitchMapMain.STATE.compareAndSet(instance, state, TERMINATED)) {
				if (instance.logger != null) {
					instance.logger.log(instance.toString(), "std", state, TERMINATED);
				}
				return state;
			}
		}
	}

	/**
	 * Atomically set main completed flag.
	 *
	 * The flag will not be set if the current state is {@link #TERMINATED}
	 *
	 * @return previous state
	 */
	static long setMainCompleted(SwitchMapMain<?, ?> instance) {
		for (; ; ) {
			final long state = instance.state;

			// do nothing if stream is terminated
			if (state == TERMINATED) {
				return TERMINATED;
			}

			if ((state & COMPLETED_MASK) == COMPLETED_MASK) {
				return state;
			}

			final long nextState = state | COMPLETED_MASK;
			if (SwitchMapMain.STATE.compareAndSet(instance, state, nextState)) {
				if (instance.logger != null) {
					instance.logger.log(instance.toString(), "smc", state, nextState);
				}
				return state;
			}
		}
	}

	/**
	 * Atomically add increment hasRequest value to indicate that we added capacity to
	 * {@link SwitchMapMain#requested}
	 *
	 * <p>
	 * Note, If the given {@code previousRequested} param value is greater than zero it
	 * works as an indicator that in some scenarios prevents setting HasRequest to 1.
	 * Due to racing nature, the {@link SwitchMapMain#requested} may be incremented but
	 * the following addRequest may be delayed. That will lead that Inner may
	 * read the new value and request more data from the inner upstream. In turn, the
	 * delay may be that big that inner may fulfill new demand and set hasRequest to 0
	 * faster that addRequest happens and leave {@link SwitchMapMain#requested} at
	 * zero as well. Thus, we use previousRequested to check if inner was requested,
	 * and if it was, we assume the explained case happened and newly added demand was
	 * already fulfilled
	 * </p>
	 *
	 * @param previousRequested received from {@link Operators#addCap} method.
	 * @return next state
	 */
	static long addRequest(SwitchMapMain<?, ?> instance, long previousRequested) {
		for (;;) {
			long state = instance.state;

			// do nothing if stream is terminated
			if (state == TERMINATED) {
				return TERMINATED;
			}

			final int hasRequest = hasRequest(state);
			// if the current hasRequest is zero and previousRequested is greater than
			// zero, it means that Inner was faster then this operation and has already
			// fulfilled the demand. In that case we do not have to increment
			// hasRequest and just need to return
			if (hasRequest == 0 && previousRequested > 0) {
				return state;
			}

			final long nextState = state(index(state), // keep index as is
					isWip(state), // keep wip flag as is
					hasRequest + 1, // increment hasRequest by 1
					isInnerSubscribed(state), // keep inner subscribed flag as is
					hasMainCompleted(state), // keep main completed flag as is
					hasInnerCompleted(state)); // keep inner completed flag as is
			if (SwitchMapMain.STATE.compareAndSet(instance, state, nextState)) {
				if (instance.logger != null) {
					instance.logger.log(instance.toString(), "adr", state, nextState);
				}
				return nextState;
			}
		}
	}

	/**
	 * Atomically increment the index of the active inner subscriber.
	 *
	 * The index will not be changed if the current state is {@link #TERMINATED}
	 *
	 * @return previous state
	 */
	static long incrementIndex(SwitchMapMain<?, ?> instance) {
		long state = instance.state;

		// do nothing if stream is terminated
		if (state == TERMINATED) {
			return TERMINATED;
		}

		// generate next index once. Main#OnNext will never race with another Main#OnNext. Also,
		// this method is only called from Main#Onnext
		int nextIndex = nextIndex(state);

		for (; ; ) {
			final long nextState = state(nextIndex, // set actual index to the next index
					isWip(state), // keep WIP flag unchanged
					hasRequest(state), // keep requested as is
					false, // unset inner subscribed flag
					false, // main completed flag can not be set at this stage
					false);// set inner completed to false since another inner comes in
			if (SwitchMapMain.STATE.compareAndSet(instance, state, nextState)) {
				if (instance.logger != null) {
					instance.logger.log(instance.toString(), "ini", state, nextState);
				}
				return state;
			}

			state = instance.state;

			// do nothing if stream is terminated
			if (state == TERMINATED) {
				return TERMINATED;
			}
		}
	}

	/**
	 * Atomically set a bit on the current state indicating that current inner has
	 * received a subscription.
	 *
	 * If the index has changed during the subscription or stream was cancelled, the bit
	 * will not bit set
	 *
	 * @return previous state
	 */
	static long setInnerSubscribed(SwitchMapMain<?, ?> instance, int expectedIndex) {
		for (;;) {
			final long state = instance.state;

			// do nothing if stream is terminated
			if (state == TERMINATED) {
				return TERMINATED;
			}

			int actualIndex = index(state);
			// do nothing if index has changed before Subscription was received
			if (expectedIndex != actualIndex) {
				return state;
			}

			final long nextState = state(expectedIndex, // keep expected index
					false, // wip assumed to be false
					hasRequest(state), // keep existing request state
					true, // set inner subscribed bit
					hasMainCompleted(state), // keep main completed flag as is
					false);// inner can not be completed at this phase
			if (SwitchMapMain.STATE.compareAndSet(instance,
					state, nextState)) {

				if (instance.logger != null) {
					instance.logger.log(instance.toString(), "sns", state, nextState);
				}

				return state;
			}
		}
	}

	/**
	 * Atomically set WIP flag to indicate that we do some work from Inner#OnNext or
	 * Inner#OnComplete.
	 *
	 * If the current state is {@link #TERMINATED} or the index is
	 * one equals to expected then the flag will not be set
	 *
	 * @return previous state
	 */
	static long setWip(SwitchMapMain<?, ?> instance, int expectedIndex) {
		for (;;) {
			final long state = instance.state;

			// do nothing if stream is terminated
			if (state == TERMINATED) {
				return TERMINATED;
			}

			int actualIndex = index(state);
			// do nothing if index has changed before Subscription was received
			if (expectedIndex != actualIndex) {
				return state;
			}

			final long nextState = state(expectedIndex, // keep expected index
					true, // set wip bit
					hasRequest(state), // keep request as is
					true, // assumed inner subscribed if index has not changed
					hasMainCompleted(state), // keep main completed flag as is
					false);// inner can not be completed at this phase
			if (SwitchMapMain.STATE.compareAndSet(instance, state, nextState)) {
				if (instance.logger != null) {
					instance.logger.log(instance.toString(), "swp", state, nextState);
				}
				return state;
			}
		}
	}

	/**
	 * Atomically unset WIP flag to indicate that we have done the work from
	 * Inner#OnNext
	 *
	 * If the current state is {@link #TERMINATED} or the index is
	 * one equals to expected then the flag will not be set
	 *
	 * @return previous state
	 */
	static long unsetWip(SwitchMapMain<?, ?> instance,
			int expectedIndex,
			boolean isDemandFulfilled,
			int expectedRequest) {
		for (; ; ) {
			final long state = instance.state;

			// do nothing if stream is terminated
			if (state == TERMINATED) {
				return TERMINATED;
			}

			final int actualIndex = index(state);
			final int actualRequest = hasRequest(state);
			final boolean sameIndex = expectedIndex == actualIndex;
			// if during onNext we fulfilled the requested demand and index has not
			// changed and it is observed that downstream requested more demand, we
			// have to repeat and see of we can request more from the inner upstream.
			// We dont have to chang WIP if we have isDemandFulfilled set to true
			if (isDemandFulfilled && expectedRequest < actualRequest && sameIndex) {
				return state;
			}

			final long nextState = state(
					actualIndex,// set actual index; assumed index may change and if we have done with the work we have to unset WIP flag
					false,// unset wip flag
					isDemandFulfilled && expectedRequest == actualRequest ? 0 : actualRequest,// set hasRequest to 0 if we know that demand was fulfilled an expectedRequest is equal to the actual one (so it has not changed)
					isInnerSubscribed(state),// keep inner state unchanged; if index has changed the flag is unset, otherwise it is set
					hasMainCompleted(state),// keep main completed flag as it is
					false); // this flag is always unset in this method  since we do call from Inner#OnNext
			if (SwitchMapMain.STATE.compareAndSet(instance, state, nextState)) {
				if (instance.logger != null) {
					instance.logger.log(instance.toString(), "uwp", state, nextState);
				}
				return state;
			}
		}
	}

	/**
	 * Atomically set the inner completed flag.
	 *
	 * The flag will not be set if the current state is {@link #TERMINATED}
	 *
	 * @return previous state
	 */
	static long setInnerCompleted(SwitchMapMain<?, ?> instance) {
		for (; ; ) {
			final long state = instance.state;

			// do nothing if stream is terminated
			if (state == TERMINATED) {
				return TERMINATED;
			}

			final boolean isInnerSubscribed = isInnerSubscribed(state);
			final long nextState = state(index(state), // keep the index unchanged
					false, // unset WIP flag
					hasRequest(state), // keep hasRequest flag as is
					isInnerSubscribed, // keep inner subscribed flag as is
					hasMainCompleted(state), // keep main completed flag as is
					isInnerSubscribed);  // if index has changed then inner subscribed remains set, thus we are safe to set inner completed flag
			if (SwitchMapMain.STATE.compareAndSet(instance, state, nextState)) {
				if (instance.logger != null) {
					instance.logger.log(instance.toString(), "sic", state, nextState);
				}
				return state;
			}
		}
	}

	/**
	 * Generic method to encode state from the given params into a long value
	 *
	 * @return encoded state
	 */
	static long state(int index, boolean wip, int hasRequest, boolean innerSubscribed,
			boolean mainCompleted, boolean innerCompleted) {
		return ((long) index << INDEX_OFFSET)
				| (wip ? INNER_WIP_MASK : 0)
				| ((long) Math.max(Math.min(hasRequest, MAX_HAS_REQUEST), 0) << HAS_REQUEST_OFFSET)
				| (innerSubscribed ? INNER_SUBSCRIBED_MASK : 0)
				| (mainCompleted ? COMPLETED_MASK : 0)
				| (innerCompleted ? INNER_COMPLETED_MASK : 0);
	}

	static boolean isInnerSubscribed(long state) {
		return (state & INNER_SUBSCRIBED_MASK) == INNER_SUBSCRIBED_MASK;
	}

	static boolean hasMainCompleted(long state) {
		return (state & COMPLETED_MASK) == COMPLETED_MASK;
	}

	static boolean hasInnerCompleted(long state) {
		return (state & INNER_COMPLETED_MASK) == INNER_COMPLETED_MASK;
	}

	static int hasRequest(long state) {
		return (int) (state & HAS_REQUEST_MASK) >> HAS_REQUEST_OFFSET;
	}

	static int index(long state) {
		return (int) (state >>> INDEX_OFFSET);
	}

	static int nextIndex(long state) {
		return ((int) (state >>> INDEX_OFFSET)) + 1;
	}

	static boolean isWip(long state) {
		return (state & INNER_WIP_MASK) == INNER_WIP_MASK;
	}
}
