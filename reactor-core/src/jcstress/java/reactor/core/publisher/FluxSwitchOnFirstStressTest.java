/*
 * Copyright (c) 2021-2024 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.Result;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.LLLLL_Result;
import org.openjdk.jcstress.infra.results.LLLL_Result;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.util.FastLogger;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public abstract class FluxSwitchOnFirstStressTest {

	final FastLogger fastLogger = new FastLogger(this.getClass().getName());

	final StressSubscription<String> inboundSubscription = new StressSubscription<>();
	final StressSubscriber<String>   inboundSubscriber   = new StressSubscriber<>(0);

	final StressSubscription<String> outboundSubscription = new StressSubscription<>();
	final StressSubscriber<String>   outboundSubscriber   = new StressSubscriber<>(0);

	final FluxSwitchOnFirst.SwitchOnFirstMain<String, String> main =
			new FluxSwitchOnFirst.SwitchOnFirstMain<String, String>(outboundSubscriber,
					this::switchOnFirst,
					false,
					new StateLogger(fastLogger));

	{
		inboundSubscription.subscribe(main);
	}

	abstract Flux<String> switchOnFirst(Signal<? extends String> signal,
			Flux<String> inbound);

	// Ignore, flaky test (https://github.com/reactor/reactor-core/issues/3627)
	//@JCStressTest
	@Outcome(id = {"1, 1, 1, 1, 1"}, expect = ACCEPTABLE)
	@State
	public static class OutboundOnSubscribeAndRequestStressTest
			extends FluxSwitchOnFirstStressTest {

		@Override
		Flux<String> switchOnFirst(Signal<? extends String> signal,
				Flux<String> inbound) {
			return new Flux<String>() {
				@Override
				public void subscribe(CoreSubscriber<? super String> actual) {
					inbound.subscribe(inboundSubscriber);
					inboundSubscriber.request(1);
					outboundSubscription.subscribe(actual);
				}
			};
		}

		@Actor
		public void next() {
			main.onNext("test");
		}

		@Actor
		public void request() {
			outboundSubscriber.request(1);
		}

		@Arbiter
		public void arbiter(LLLLL_Result result) {
			result.r1 = outboundSubscription.requestsCount;
			result.r2 = outboundSubscription.requested;
			result.r3 = inboundSubscription.requestsCount;
			result.r4 = inboundSubscription.requested;
			result.r5 = inboundSubscriber.onNextCalls;
		}
	}

	// Ignore, flaky test (https://github.com/reactor/reactor-core/issues/3627)
	//@JCStressTest
	@Outcome(id = {
			"0, 0, 1, 2, 2, 0, 1, 1, 0"}, expect = ACCEPTABLE, desc = "Inbound got second request, delivered onNext('value') and delivered onComplete() before cancellation")
	@Outcome(id = {
			"0, 0, 1, 2, 2, 1, 2, 1, 0"}, expect = ACCEPTABLE, desc = "Inbound got second request, delivered onNext('value') but got cancel before onComplete(). CancellationException was propagated to the inboundSubscriber")
	@Outcome(id = {
			"0, 0, 1, 1, 1, 1, 2, 0, 1"}, expect = ACCEPTABLE, desc = "Cancellation happened as the earliest event. firstValue is discarded")
	@State
	public static class InboundSubscribeAndOutboundCancelAndInboundCompleteStressTest
			extends FluxSwitchOnFirstStressTest {

		Flux<String> inboundStream;

		@Override
		Flux<String> switchOnFirst(Signal<? extends String> signal,
				Flux<String> inbound) {
			return new Flux<String>() {
				@Override
				public void subscribe(CoreSubscriber<? super String> actual) {
					inboundStream = inbound;
					outboundSubscription.subscribe(actual);
				}
			};
		}

		{
			main.onNext("value");
		}

		@Actor
		public void completeInbound() {
			main.onComplete();
		}

		@Actor
		public void subscribeInbound() {
			inboundStream.subscribe(inboundSubscriber);
			inboundSubscriber.request(2);
		}

		@Actor
		public void cancelOutbound() {
			outboundSubscriber.cancel();
		}

		@Arbiter
		public void arbiter(LLLLLLLLL_Result result) {
			result.r1 = outboundSubscription.requestsCount;
			result.r2 = outboundSubscription.requested;
			result.r3 = outboundSubscription.cancelled ? 1 : 0;

			result.r4 = inboundSubscription.requestsCount;
			result.r5 = inboundSubscription.requested;
			result.r6 = inboundSubscription.cancelled ? 1 : 0;

			result.r7 =
					inboundSubscriber.onCompleteCalls.get() + inboundSubscriber.onErrorCalls.get() * 2;
			result.r8 = inboundSubscriber.onNextCalls;
			result.r9 = outboundSubscriber.onNextDiscarded;

			if (inboundSubscriber.concurrentOnError.get()) {
				throw new RuntimeException("Concurrent OnError");
			}
			if (inboundSubscriber.concurrentOnNext.get()) {
				throw new RuntimeException("Concurrent OnNext");
			}
			if (inboundSubscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("Concurrent OnComplete");
			}
		}
	}

	// Ignore, flaky test (https://github.com/reactor/reactor-core/issues/3627)
	//@JCStressTest
	@Outcome(id = {
			"0, 0, 1, 2, 2, 0, 2, 1, 0"}, expect = ACCEPTABLE, desc = "Inbound got second request, delivered onNext('value') and delivered onError() before cancellation")
	@Outcome(id = {
			"0, 0, 1, 2, 2, 1, 5, 1, 0"}, expect = ACCEPTABLE, desc = "Inbound got second request, delivered onNext('value') but got cancel before onError(). CancellationException was propagated to the inboundSubscriber")
	@Outcome(id = {
			"0, 0, 1, 1, 1, 1, 5, 0, 1"}, expect = ACCEPTABLE, desc = "Cancellation happened as the earliest event. firstValue is discarded")
	@State
	public static class InboundSubscribeAndOutboundCancelAndInboundErrorStressTest
			extends FluxSwitchOnFirstStressTest {

		static final RuntimeException DUMMY_ERROR = new RuntimeException("dummy");
		Flux<String> inboundStream;

		@Override
		Flux<String> switchOnFirst(Signal<? extends String> signal,
				Flux<String> inbound) {
			return new Flux<String>() {
				@Override
				public void subscribe(CoreSubscriber<? super String> actual) {
					inboundStream = inbound;
					outboundSubscription.subscribe(actual);
				}
			};
		}

		{
			main.onNext("value");
		}

		@Actor
		public void errorInbound() {
			main.onError(DUMMY_ERROR);
		}

		@Actor
		public void subscribeInbound() {
			inboundStream.subscribe(inboundSubscriber);
			inboundSubscriber.request(2);
		}

		@Actor
		public void cancelOutbound() {
			outboundSubscriber.cancel();
		}

		@Arbiter
		public void arbiter(LLLLLLLLL_Result result) {
			result.r1 = outboundSubscription.requestsCount;
			result.r2 = outboundSubscription.requested;
			result.r3 = outboundSubscription.cancelled ? 1 : 0;

			result.r4 = inboundSubscription.requestsCount;
			result.r5 = inboundSubscription.requested;
			result.r6 = inboundSubscription.cancelled ? 1 : 0;

			result.r7 =
					inboundSubscriber.onCompleteCalls.get() + inboundSubscriber.onErrorCalls.get() * 2 + outboundSubscriber.droppedErrors.size() * 3;
			result.r8 = inboundSubscriber.onNextCalls;
			result.r9 = outboundSubscriber.onNextDiscarded;

			if (inboundSubscriber.concurrentOnError.get()) {
				throw new RuntimeException("Concurrent OnError");
			}
			if (inboundSubscriber.concurrentOnNext.get()) {
				throw new RuntimeException("Concurrent OnNext");
			}
			if (inboundSubscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("Concurrent OnComplete");
			}
		}
	}

	// Ignore, flaky test (https://github.com/reactor/reactor-core/issues/3627)
	//@JCStressTest
	@Outcome(id = {
			"1, 2, 0, 1"}, expect = ACCEPTABLE, desc = "inbound next with error happens first")
	@Outcome(id = {
			"1, 0, 0, 1"}, expect = ACCEPTABLE, desc = "cancellation happened first")
	@Outcome(id = {
			"1, 3, 0, 1"}, expect = ACCEPTABLE, desc = "cancellation in between")
	@State
	public static class InboundNextLeadingToErrorAndOutboundCancelStressTest
			extends FluxSwitchOnFirstStressTest {

		static final RuntimeException DUMMY_ERROR = new RuntimeException("dummy");

		@Override
		Flux<String> switchOnFirst(Signal<? extends String> signal,
				Flux<String> inbound) {
			throw DUMMY_ERROR;
		}

		@Actor
		public void nextInbound() {
			main.onNext("value");
		}

		@Actor
		public void cancelOutbound() {
			outboundSubscriber.cancel();
		}

		@Arbiter
		public void arbiter(LLLL_Result result) {
			result.r1 = inboundSubscription.cancelled ? 1 : 0;
			result.r2 =
					outboundSubscriber.onCompleteCalls.get() + outboundSubscriber.onErrorCalls.get() * 2 + outboundSubscriber.droppedErrors.size() * 3;
			result.r3 = outboundSubscriber.onNextCalls;
			result.r4 = outboundSubscriber.onNextDiscarded;

			if (outboundSubscriber.concurrentOnError.get()) {
				throw new RuntimeException("Concurrent OnError");
			}
			if (outboundSubscriber.concurrentOnNext.get()) {
				throw new RuntimeException("Concurrent OnNext");
			}
			if (outboundSubscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("Concurrent OnComplete");
			}
		}
	}

	// Ignore, flaky test (https://github.com/reactor/reactor-core/issues/3627)
	//@JCStressTest
	@Outcome(id = {
			"0, 2, 0, 0"}, expect = ACCEPTABLE, desc = "inbound error with transformation error happens first")
	@Outcome(id = {
			"1, 3, 0, 0"}, expect = ACCEPTABLE, desc = "cancellation happened first")
	@Outcome(id = {
			"0, 3, 0, 0"}, expect = ACCEPTABLE, desc = "cancellation happened in between")
	@State
	public static class InboundErrorLeadingToErrorAndOutboundCancelStressTest
			extends FluxSwitchOnFirstStressTest {

		static final RuntimeException DUMMY_ERROR_1 = new RuntimeException("dummy1");
		static final RuntimeException DUMMY_ERROR_2 = new RuntimeException("dummy2");

		@Override
		Flux<String> switchOnFirst(Signal<? extends String> signal,
				Flux<String> inbound) {
			throw DUMMY_ERROR_2;
		}

		@Actor
		public void errorInbound() {
			main.onError(DUMMY_ERROR_1);
		}

		@Actor
		public void cancelOutbound() {
			outboundSubscriber.cancel();
		}

		@Arbiter
		public void arbiter(LLLL_Result result) {
			result.r1 = inboundSubscription.cancelled ? 1 : 0;
			result.r2 =
					outboundSubscriber.onCompleteCalls.get() + outboundSubscriber.onErrorCalls.get() * 2 + outboundSubscriber.droppedErrors.size() * 3;
			result.r3 = outboundSubscriber.onNextCalls;
			result.r4 = outboundSubscriber.onNextDiscarded;

			if (outboundSubscriber.concurrentOnError.get()) {
				throw new RuntimeException("Concurrent OnError");
			}
			if (outboundSubscriber.concurrentOnNext.get()) {
				throw new RuntimeException("Concurrent OnNext");
			}
			if (outboundSubscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("Concurrent OnComplete");
			}
		}
	}

	// Ignore, flaky test (https://github.com/reactor/reactor-core/issues/3627)
	//@JCStressTest
	@Outcome(id = {
			"0, 2, 0, 0"}, expect = ACCEPTABLE, desc = "inbound complete with transformation error happens first")
	@Outcome(id = {
			"1, 3, 0, 0"}, expect = ACCEPTABLE, desc = "cancellation happened first")
	@Outcome(id = {
			"0, 3, 0, 0"}, expect = ACCEPTABLE, desc = "cancellation happened in between")
	@State
	public static class InboundCompleteLeadingToErrorAndOutboundCancelStressTest
			extends FluxSwitchOnFirstStressTest {

		static final RuntimeException DUMMY_ERROR_1 = new RuntimeException("dummy1");
		static final RuntimeException DUMMY_ERROR_2 = new RuntimeException("dummy2");

		@Override
		Flux<String> switchOnFirst(Signal<? extends String> signal,
				Flux<String> inbound) {
			throw DUMMY_ERROR_2;
		}

		@Actor
		public void errorInbound() {
			main.onError(DUMMY_ERROR_1);
		}

		@Actor
		public void cancelOutbound() {
			outboundSubscriber.cancel();
		}

		@Arbiter
		public void arbiter(LLLL_Result result) {
			result.r1 = inboundSubscription.cancelled ? 1 : 0;
			result.r2 =
					outboundSubscriber.onCompleteCalls.get() + outboundSubscriber.onErrorCalls.get() * 2 + outboundSubscriber.droppedErrors.size() * 3;
			result.r3 = outboundSubscriber.onNextCalls;
			result.r4 = outboundSubscriber.onNextDiscarded;

			if (outboundSubscriber.concurrentOnError.get()) {
				throw new RuntimeException("Concurrent OnError");
			}
			if (outboundSubscriber.concurrentOnNext.get()) {
				throw new RuntimeException("Concurrent OnNext");
			}
			if (outboundSubscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("Concurrent OnComplete");
			}
		}
	}

	// Ignore, flaky test (https://github.com/reactor/reactor-core/issues/3627)
	//@JCStressTest
	@Outcome(id = {
			"0, 0, 1, 2, 2, 1, 0, 1, 0"}, expect = ACCEPTABLE, desc = "inbound request happened first. then inbound cancel. then outbound cancel")
	@Outcome(id = {
			"0, 0, 1, 2, 2, 1, 2, 1, 0"}, expect = ACCEPTABLE, desc = "inbound request happened first. then outbound cancel with error")
	@Outcome(id = {
			"0, 0, 1, 1, 1, 1, 0, 0, 1"}, expect = ACCEPTABLE, desc = "inbound cancel first")
	@Outcome(id = {
			"0, 0, 1, 1, 1, 1, 2, 0, 1"}, expect = ACCEPTABLE, desc = "outbound cancel with error first")
	@State
	public static class OutboundCancelAndInboundCancelStressTest
			extends FluxSwitchOnFirstStressTest {

		Flux<String> inboundStream;

		@Override
		Flux<String> switchOnFirst(Signal<? extends String> signal,
				Flux<String> inbound) {
			return new Flux<String>() {
				@Override
				public void subscribe(CoreSubscriber<? super String> actual) {
					inboundStream = inbound;
					outboundSubscription.subscribe(actual);
				}
			};
		}

		{
			main.onNext("value");
			inboundStream.subscribe(inboundSubscriber);
		}

		@Actor
		public void cancelInbound() {
			inboundSubscriber.cancel();
		}

		@Actor
		public void requestInbound() {
			inboundSubscriber.request(2);
		}

		@Actor
		public void cancelOutbound() {
			outboundSubscriber.cancel();
		}

		@Arbiter
		public void arbiter(LLLLLLLLL_Result result) {
			result.r1 = outboundSubscription.requestsCount;
			result.r2 = outboundSubscription.requested;
			result.r3 = outboundSubscription.cancelled ? 1 : 0;

			result.r4 = inboundSubscription.requestsCount;
			result.r5 = inboundSubscription.requested;
			result.r6 = inboundSubscription.cancelled ? 1 : 0;

			result.r7 =
					inboundSubscriber.onCompleteCalls.get() + inboundSubscriber.onErrorCalls.get() * 2 + outboundSubscriber.droppedErrors.size() * 3;
			result.r8 = inboundSubscriber.onNextCalls;
			result.r9 = outboundSubscriber.onNextDiscarded;

			if (inboundSubscriber.concurrentOnError.get()) {
				throw new RuntimeException("Concurrent OnError");
			}
			if (inboundSubscriber.concurrentOnNext.get()) {
				throw new RuntimeException("Concurrent OnNext");
			}
			if (inboundSubscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("Concurrent OnComplete");
			}
		}
	}

	// Ignore, flaky test (https://github.com/reactor/reactor-core/issues/3627)
	//@JCStressTest
	@Outcome(id = {
			"1, 1, 2, 1, 1"}, expect = ACCEPTABLE, desc = "outbound cancel happened before inbound next")
	@Outcome(id = {
			"1, 1, 2, 2, 0"}, expect = ACCEPTABLE, desc = "inbound next happened before outbound cancel")
	@State
	public static class OutboundCancelAndInboundNextStressTest
			extends FluxSwitchOnFirstStressTest {

		Flux<String> inboundStream;

		@Override
		Flux<String> switchOnFirst(Signal<? extends String> signal,
				Flux<String> inbound) {
			return new Flux<String>() {
				@Override
				public void subscribe(CoreSubscriber<? super String> actual) {
					inboundStream = inbound;
					outboundSubscription.subscribe(actual);
				}
			};
		}

		{
			main.onNext("value");
			inboundStream.subscribe(inboundSubscriber);
			inboundSubscriber.request(2);
		}

		@Actor
		public void nextInbound() {
			main.onNext("value2");
		}

		@Actor
		public void cancelOutbound() {
			outboundSubscriber.cancel();
		}

		@Arbiter
		public void arbiter(LLLLL_Result result) {
			result.r1 = outboundSubscription.cancelled ? 1 : 0;
			result.r2 = inboundSubscription.cancelled ? 1 : 0;

			result.r3 =
					inboundSubscriber.onCompleteCalls.get() + inboundSubscriber.onErrorCalls.get() * 2 + outboundSubscriber.droppedErrors.size() * 3;
			result.r4 = inboundSubscriber.onNextCalls;
			result.r5 = outboundSubscriber.onNextDiscarded;

			if (inboundSubscriber.concurrentOnError.get()) {
				throw new RuntimeException("Concurrent OnError");
			}
			if (inboundSubscriber.concurrentOnNext.get()) {
				throw new RuntimeException("Concurrent OnNext");
			}
			if (inboundSubscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("Concurrent OnComplete");
			}
		}
	}

	// Ignore, flaky test (https://github.com/reactor/reactor-core/issues/3627)
	//@JCStressTest
	@Outcome(id = {
			"1, 0, 1, 1, 0"}, expect = ACCEPTABLE, desc = "inbound complete happened before outbound cancel")
	@Outcome(id = {
			"1, 1, 2, 1, 0"}, expect = ACCEPTABLE, desc = "outbound cancel happened before inbound complete")
	@State
	public static class OutboundCancelAndInboundCompleteStressTest
			extends FluxSwitchOnFirstStressTest {

		Flux<String> inboundStream;

		@Override
		Flux<String> switchOnFirst(Signal<? extends String> signal,
				Flux<String> inbound) {
			return new Flux<String>() {
				@Override
				public void subscribe(CoreSubscriber<? super String> actual) {
					inboundStream = inbound;
					outboundSubscription.subscribe(actual);
				}
			};
		}

		{
			main.onNext("value");
			inboundStream.subscribe(inboundSubscriber);
			inboundSubscriber.request(2);
		}

		@Actor
		public void nextInbound() {
			main.onComplete();
		}

		@Actor
		public void cancelOutbound() {
			outboundSubscriber.cancel();
		}

		@Arbiter
		public void arbiter(LLLLL_Result result) {
			result.r1 = outboundSubscription.cancelled ? 1 : 0;
			result.r2 = inboundSubscription.cancelled ? 1 : 0;

			result.r3 =
					inboundSubscriber.onCompleteCalls.get() + inboundSubscriber.onErrorCalls.get() * 2 + outboundSubscriber.droppedErrors.size() * 3;
			result.r4 = inboundSubscriber.onNextCalls;
			result.r5 = outboundSubscriber.onNextDiscarded;

			if (inboundSubscriber.concurrentOnError.get()) {
				throw new RuntimeException("Concurrent OnError");
			}
			if (inboundSubscriber.concurrentOnNext.get()) {
				throw new RuntimeException("Concurrent OnNext");
			}
			if (inboundSubscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("Concurrent OnComplete");
			}
		}
	}

	// Ignore, flaky test (https://github.com/reactor/reactor-core/issues/3627)
	//@JCStressTest
	@Outcome(id = {
			"1, 0, 2, 1, 0"}, expect = ACCEPTABLE, desc = "inbound error happened before outbound cancel")
	@Outcome(id = {
			"1, 1, 5, 1, 0"}, expect = ACCEPTABLE, desc = "outbound cancel happened before inbound error")
	@State
	public static class OutboundCancelAndInboundErrorStressTest
			extends FluxSwitchOnFirstStressTest {

		static final RuntimeException DUMMY_ERROR = new RuntimeException("dummy");

		Flux<String> inboundStream;

		@Override
		Flux<String> switchOnFirst(Signal<? extends String> signal,
				Flux<String> inbound) {
			return new Flux<String>() {
				@Override
				public void subscribe(CoreSubscriber<? super String> actual) {
					inboundStream = inbound;
					outboundSubscription.subscribe(actual);
				}
			};
		}

		{
			main.onNext("value");
			inboundStream.subscribe(inboundSubscriber);
			inboundSubscriber.request(2);
		}

		@Actor
		public void nextInbound() {
			main.onError(DUMMY_ERROR);
		}

		@Actor
		public void cancelOutbound() {
			outboundSubscriber.cancel();
		}

		@Arbiter
		public void arbiter(LLLLL_Result result) {
			result.r1 = outboundSubscription.cancelled ? 1 : 0;
			result.r2 = inboundSubscription.cancelled ? 1 : 0;

			result.r3 =
					inboundSubscriber.onCompleteCalls.get() + inboundSubscriber.onErrorCalls.get() * 2 + outboundSubscriber.droppedErrors.size() * 3;
			result.r4 = inboundSubscriber.onNextCalls;
			result.r5 = outboundSubscriber.onNextDiscarded;

			if (inboundSubscriber.concurrentOnError.get()) {
				throw new RuntimeException("Concurrent OnError");
			}
			if (inboundSubscriber.concurrentOnNext.get()) {
				throw new RuntimeException("Concurrent OnNext");
			}
			if (inboundSubscriber.concurrentOnComplete.get()) {
				throw new RuntimeException("Concurrent OnComplete");
			}
		}
	}

	static class StressSubscription<T> implements Subscription {

		CoreSubscriber<? super T> actual;

		public volatile int subscribes;

		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<StressSubscription> SUBSCRIBES =
				AtomicIntegerFieldUpdater.newUpdater(StressSubscription.class,
						"subscribes");

		public volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<StressSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(StressSubscription.class, "requested");

		public volatile int requestsCount;

		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<StressSubscription> REQUESTS_COUNT =
				AtomicIntegerFieldUpdater.newUpdater(StressSubscription.class,
						"requestsCount");

		public volatile boolean cancelled;

		void subscribe(CoreSubscriber<? super T> actual) {
			this.actual = actual;
			actual.onSubscribe(this);
			SUBSCRIBES.getAndIncrement(this);
		}

		@Override
		public void request(long n) {
			REQUESTS_COUNT.incrementAndGet(this);
			Operators.addCap(REQUESTED, this, n);
		}

		@Override
		public void cancel() {
			cancelled = true;
		}

	}

	@Result
	public static final class LLLLLLLLL_Result implements Serializable {

		@sun.misc.Contended
		@jdk.internal.vm.annotation.Contended
		public Object r1;

		@sun.misc.Contended
		@jdk.internal.vm.annotation.Contended
		public Object r2;

		@sun.misc.Contended
		@jdk.internal.vm.annotation.Contended
		public Object r3;

		@sun.misc.Contended
		@jdk.internal.vm.annotation.Contended
		public Object r4;

		@sun.misc.Contended
		@jdk.internal.vm.annotation.Contended
		public Object r5;

		@sun.misc.Contended
		@jdk.internal.vm.annotation.Contended
		public Object r6;

		@sun.misc.Contended
		@jdk.internal.vm.annotation.Contended
		public Object r7;

		@sun.misc.Contended
		@jdk.internal.vm.annotation.Contended
		public Object r8;

		@sun.misc.Contended
		@jdk.internal.vm.annotation.Contended
		public Object r9;

		@sun.misc.Contended
		@jdk.internal.vm.annotation.Contended
		public int jcstress_trap; // reserved for infrastructure use

		public int hashCode() {
			return 0 + 0 << 1 + 0 << 2 + 0 << 3 + 0 << 4 + 0 << 5 + 0 << 6 + 0 << 7 + 0 << 8;
		}

		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			LLLLLLLLL_Result that = (LLLLLLLLL_Result) o;
			return true;
		}

		public String toString() {
			return "" + r1 + ", " + r2 + ", " + r3 + ", " + r4 + ", " + r5 + ", " + r6 + ", " + r7 + ", " + r8 + ", " + r9;
		}
	}
}
