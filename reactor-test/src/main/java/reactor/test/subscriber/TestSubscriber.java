/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.test.subscriber;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Signal;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * A {@link CoreSubscriber} that can be attached to any {@link org.reactivestreams.Publisher} to later assert which
 * events occurred at runtime. This is an alternative to {@link reactor.test.StepVerifier} which allows to evaluate
 * more complex scenarios where there are more than one possible outcome (eg. racing).
 * <p>
 * The subscriber can be fine tuned {@link #withOptions()}, which also allows to produce a {@link reactor.core.Fuseable.ConditionalSubscriber}
 * variant, a {@link reactor.core.Fuseable} variant and third variant that combines both interfaces, if needed.
 *
 * @author Simon Baslé
 */
public class TestSubscriber<T> implements CoreSubscriber<T>, Scannable {

	/**
	 * Create a simple plain {@link TestSubscriber} which will make an unbounded demand {@link #onSubscribe(Subscription) on subscription},
	 * has an empty {@link Context} and makes no attempt at fusion negotiation.
	 *
	 * @param <T> the type of data received by this subscriber
	 * @return a new {@link TestSubscriber}
	 */
	static <T> TestSubscriber<T> create() {
		return new TestSubscriber<>(new TestSubscriberSpec());
	}

	/**
	 * An enum representing the 3 broad expectations around fusion.
	 */
	public enum FusionRequirement {

		/**
		 * The parent {@link org.reactivestreams.Publisher} is expected to be fuseable, and this is
		 * verified by checking the {@link Subscription} it provides is a {@link reactor.core.Fuseable.QueueSubscription}.
		 */
		REQUIRE_FUSEABLE,
		/**
		 * The parent {@link org.reactivestreams.Publisher} is expected to NOT be fuseable, and this is
		 * verified by checking the {@link Subscription} it provides is NOT a {@link reactor.core.Fuseable.QueueSubscription}.
		 */
		REQUIRE_NOT_FUSEABLE,
		/**
		 * There is no particular interest in the fuseability of the parent {@link org.reactivestreams.Publisher},
		 * so even if it provides a {@link reactor.core.Fuseable.QueueSubscription} it will be used as a
		 * vanilla {@link Subscription}.
		 */
		NO_REQUIREMENT;

	}

	/**
	 * Create a {@link TestSubscriber} with tuning. See {@link TestSubscriberSpec}.
	 *
	 * @return a {@link TestSubscriberSpec} to fine tune the {@link TestSubscriber} to produce
	 */
	public static TestSubscriberSpec withOptions() {
		return new TestSubscriberSpec();
	}

	final long                             initialRequest;
	final Context                          context;
	final TestSubscriber.FusionRequirement fusionRequirement;
	final int                              requestedFusionMode;
	final int                              expectedFusionMode;

	Subscription s;
	@Nullable
	Fuseable.QueueSubscription<T> qs;
	int fusionMode = -1;

	// state tracking
	AtomicBoolean cancelled;
	final List<T> receivedOnNext;
	final List<T> receivedPostCancellation;
	final List<Signal<T>> protocolErrors;
	final AtomicReference<Signal<T>> terminalSignal;

	final CountDownLatch doneLatch;
	final AtomicReference<AssertionError> internalFailure;

	TestSubscriber(TestSubscriberSpec options) {
		this.initialRequest = options.initialRequest;
		this.context = options.context;
		this.fusionRequirement = options.fusionRequirement;
		this.requestedFusionMode = options.requestedFusionMode;
		this.expectedFusionMode = options.expectedFusionMode;

		this.cancelled = new AtomicBoolean();
		this.receivedOnNext = new CopyOnWriteArrayList<>();
		this.receivedPostCancellation = new CopyOnWriteArrayList<>();
		this.protocolErrors = new CopyOnWriteArrayList<>();
		this.terminalSignal = new AtomicReference<>();

		this.doneLatch = new CountDownLatch(1);
		this.internalFailure = new AtomicReference<>();
	}

	@Override
	public Context currentContext() {
		return this.context;
	}

	void internalCancel() {
		Subscription s = this.s;
		if (cancelled.compareAndSet(false, true) && s != null) {
			s.cancel();
		}
	}

	void internalFail(String message) {
		if (this.internalFailure.compareAndSet(null, new AssertionError(message))) {
			internalCancel();
			notifyDone();
		}
	}

	private void notifyDone() {
		doneLatch.countDown();
	}

	@Override
	public void onSubscribe(Subscription s) {
		if (cancelled.get()) {
			s.cancel();
			return;
		}
		if (!Operators.validate(this.s, s)) {
			this.protocolErrors.add(Signal.subscribe(s));
			return;
		}
		this.s = s;
		this.fusionMode = -1;
		if (s instanceof Fuseable.QueueSubscription) {
			if (fusionRequirement == FusionRequirement.REQUIRE_NOT_FUSEABLE) {
				internalFail("TestSubscriber configured to reject QueueSubscription, got " + s);
				return;
			}

			@SuppressWarnings("unchecked") //intermediate variable to suppress via annotation for compiler's benefit
			Fuseable.QueueSubscription<T> converted = (Fuseable.QueueSubscription<T>) s;
			this.qs = converted;
			int negotiatedMode = qs.requestFusion(this.requestedFusionMode);

			if (expectedFusionMode != negotiatedMode && expectedFusionMode != Fuseable.ANY) {
				internalFail("TestSubscriber negotiated fusion mode inconsistent, expected " +
						Fuseable.fusionModeName(expectedFusionMode) + " got " + Fuseable.fusionModeName(negotiatedMode));
				return;
			}
			this.fusionMode = negotiatedMode;
			if (negotiatedMode == Fuseable.SYNC) {
				for (;;) {
					if (cancelled.get()) {
						break;
					}
					T v = qs.poll();
					if (v == null) {
						onComplete();
						break;
					}
					onNext(v);
				}
			}
			else if (this.initialRequest > 0L) {
				s.request(this.initialRequest);
			}
		}
		else if (fusionRequirement == FusionRequirement.REQUIRE_FUSEABLE) {
			internalFail("TestSubscriber configured to require QueueSubscription, got " + s);
		}
		else if (this.initialRequest > 0L) {
			s.request(this.initialRequest);
		}
	}

	@Override
	public void onNext(@Nullable T t) {
		if (terminalSignal.get() != null) {
			if (t != null) {
				this.protocolErrors.add(Signal.next(t));
			}
			else {
				this.protocolErrors.add(Signal.error(
						new AssertionError("onNext(null) received despite SYNC fusion (which has already completed)")
				));
			}
			return;
		}

		if (t == null) {
			if (this.fusionMode == Fuseable.ASYNC) {
				assert this.qs != null;
				for (; ; ) {
					if (cancelled.get()) {
						return;
					}
					t = qs.poll();
					if (t == null) {
						//no more available data, until next request (or termination)
						return;
					}
					this.receivedOnNext.add(t);
				}
			}
			else {
				internalFail("onNext(null) received while ASYNC fusion not established");
			}
		}

		this.receivedOnNext.add(t);
		if (cancelled.get()) {
			this.receivedPostCancellation.add(t);
		}
	}

	@Override
	public void onComplete() {
		Signal<T> sig = Signal.complete();
		if (terminalSignal.compareAndSet(null, sig)) {
			notifyDone();
		}
		else {
			this.protocolErrors.add(sig);
		}
	}

	@Override
	public void onError(Throwable t) {
		Signal<T> sig = Signal.error(t);
		if (terminalSignal.compareAndSet(null, sig)) {
			notifyDone();
		}
		else {
			this.protocolErrors.add(sig);
		}
	}

	@Nullable
	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.TERMINATED) return terminalSignal.get() != null;
		if (key == Attr.CANCELLED) return cancelled.get();
		if (key == Attr.ERROR) {
			Signal<T> sig = terminalSignal.get();
			if (sig == null || sig.getThrowable() == null) {
				return null;
			}
			return sig.getThrowable();
		}
		if (key == Attr.PARENT) return s;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	// == public subscription-like methods

	public void cancel() {
		if (cancelled.compareAndSet(false, true)) {
			if (this.s != null) {
				this.s.cancel();
			}
			notifyDone();
		}
	}

	public void request(long n) {
		if (this.s == null) {
			throw new IllegalStateException("Request can only happen once a Subscription has been established." +
					"Have you subscribed the TestSubscriber?");
		}
		if (Operators.validate(n)) {
			if (this.fusionMode == Fuseable.SYNC) {
				internalCancel();
				throw new IllegalStateException("Request is short circuited in SYNC fusion mode, and should not be explicitly used");
			}
			this.s.request(n);
		}
	}

	// == public accessors

	public boolean isTerminated() {
		return terminalSignal.get() != null;
	}

	public boolean isCancelled() {
		return cancelled.get();
	}

	public boolean isTerminatedOrCancelled() {
		return terminalSignal.get() != null || cancelled.get();
	}

	@Nullable
	public Signal<T> getTerminalSignal() {
		return this.terminalSignal.get();
	}

	public Signal<T> checkTerminalSignal() {
		Signal<T> sig = terminalSignal.get();
		if (sig == null || (!sig.isOnError() && !sig.isOnComplete())) {
			cancel();
			throw new AssertionError("Expected subscriber to be terminated, but it has not been terminated yet.");
		}
		return sig;
	}

	public Throwable checkTerminalError() {
		Signal<T> sig = terminalSignal.get();
		if (sig == null) {
			cancel();
			throw new AssertionError("Expected subscriber to have errored, but it has not been terminated yet.");
		}
		if (sig.isOnComplete()) {
			throw new AssertionError("Expected subscriber to have errored, but it has completed instead.");
		}
		Throwable terminal = sig.getThrowable();
		if (terminal == null) {
			cancel();
			throw new AssertionError("Expected subscriber to have errored, got unexpected terminal signal <" + sig + ">.");
		}
		return terminal;
	}

	public List<T> getReceivedOnNext() {
		return new ArrayList<>(this.receivedOnNext);
	}

	public List<T> getOnNextSublistReceivedAfterCancel() {
		return new ArrayList<>(this.receivedPostCancellation);
	}

	/**
	 * Note that the {@link Signal} in the collection don't bear any {@link reactor.util.context.ContextView},
	 * since they would all be the configured {@link #currentContext()}.
	 * @return
	 */
	public List<Signal<T>> getProtocolErrors() {
		return new ArrayList<>(this.protocolErrors);
	}

	public int getFusionMode() {
		return this.fusionMode;
	}


	// == blocking and awaiting termination

	public void block() {
		try {
			this.doneLatch.await();
			AssertionError internalFailure = this.internalFailure.get();
			if (internalFailure != null) {
				throw internalFailure;
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError("Block() interrupted", e);
		}
	}

	public void block(Duration timeout) {
		long timeoutMs = timeout.toMillis();
		try {
			boolean done = this.doneLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
			AssertionError internalFailure = this.internalFailure.get();
			if (internalFailure != null) {
				throw internalFailure;
			}
			if (!done) {
				throw new AssertionError("TestSubscriber timed out, not terminated after " + timeout + " (" + timeoutMs + "ms)");
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError("Block(" + timeout +") interrupted", e);
		}
	}

}
