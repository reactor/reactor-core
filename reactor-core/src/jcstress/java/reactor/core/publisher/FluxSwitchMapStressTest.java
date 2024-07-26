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

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.III_Result;
import org.openjdk.jcstress.infra.results.IIL_Result;
import org.openjdk.jcstress.infra.results.II_Result;
import org.openjdk.jcstress.infra.results.IZL_Result;
import org.openjdk.jcstress.infra.results.I_Result;
import org.openjdk.jcstress.infra.results.JI_Result;
import org.openjdk.jcstress.infra.results.JJJJJJJ_Result;
import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.FluxSwitchMapNoPrefetch.SwitchMapMain;
import reactor.core.util.FastLogger;
import reactor.test.publisher.TestPublisher;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public abstract class FluxSwitchMapStressTest {

	final FastLogger fastLogger = new FastLogger(this.getClass().getSimpleName());
	final StateLogger logger = new StateLogger(fastLogger);

	final StressSubscriber<Object> stressSubscriber = new StressSubscriber<>(0);
	final StressSubscription stressSubscription = new StressSubscription(null);

	final SwitchMapMain<Object, Object> switchMapMain =
			new SwitchMapMain<>(stressSubscriber, this::handle, logger);


	abstract Publisher<Object> handle(Object value);

	@JCStressTest
	@Outcome(id = {"1"}, expect = ACCEPTABLE, desc = "Exactly one onComplete")
	@State
	public static class OnCompleteStressTest extends FluxSwitchMapStressTest {

		final TestPublisher<Object> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		{
			switchMapMain.onNext("1");
			switchMapMain.request(1);
		}

		@Override
		Publisher<Object> handle(Object value) {
			return testPublisher;
		}

		@Actor
		public void outerProducer() {
			switchMapMain.onComplete();
		}

		@Actor
		public void innerProducer() {
			testPublisher.next(1);
			testPublisher.complete();
		}

		@Arbiter
		public void arbiter(I_Result r) {
			r.r1 = stressSubscriber.onCompleteCalls.get();
		}
	}

	@JCStressTest
	@Outcome(id = {"0, true, -1"}, expect = ACCEPTABLE, desc = "Cancellation with no downstream onComplete")
	@Outcome(id = {"0, false, -1"}, expect = ACCEPTABLE, desc = "Upstream completion with inner cancellation with no downstream onComplete")
	@Outcome(id = {"1, false, -1"}, expect = ACCEPTABLE, desc = "Upstream completion with inner completion with onComplete")
	@State
	public static class CancelInnerCompleteStressTest extends FluxSwitchMapStressTest {

		final TestPublisher<Object> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION, TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		{
			switchMapMain.onSubscribe(stressSubscription);
			switchMapMain.onNext("1");
			switchMapMain.request(1);
		}

		@Override
		Publisher<Object> handle(Object value) {
			return testPublisher;
		}

		@Actor
		public void outerProducer() {
			switchMapMain.onComplete();
		}

		@Actor
		public void outerRequest() {
			testPublisher.next(1);
			testPublisher.complete();
		}

		@Actor
		public void outerCancel() {
			switchMapMain.cancel();
		}

		@Arbiter
		public void arbiter(IZL_Result r) {
			r.r1 = stressSubscriber.onCompleteCalls.get();
			r.r2 = stressSubscription.cancelled.get();
			r.r3 = switchMapMain.state;
		}
	}

	@JCStressTest
	@Outcome(id = {"0, 1"}, expect = ACCEPTABLE, desc = "Cancellation with no onError and dropped Error")
	@Outcome(id = {"1, 0"}, expect = ACCEPTABLE, desc = "onError happened first")
	@State
	public static class CancelInnerErrorStressTest extends FluxSwitchMapStressTest {

		final Throwable t = new RuntimeException("test");

		StressSubscription<Integer> subscription;

		{
			switchMapMain.onSubscribe(stressSubscription);
			switchMapMain.onNext("1");
			switchMapMain.request(1);
		}

		@Override
		Publisher<Object> handle(Object value) {
			return s -> {
				final StressSubscription subscription =
						new StressSubscription<>((CoreSubscriber) s);
				this.subscription = subscription;
				s.onSubscribe(subscription);
			};
		}

		@Actor
		public void outerProducer() {
			switchMapMain.cancel();
		}

		@Actor
		public void innerProducer() {
			subscription.actual.onNext(1);
			subscription.actual.onError(t);
		}

		@Arbiter
		public void arbiter(II_Result r) {
			Hooks.resetOnErrorDropped();
			r.r1 = stressSubscriber.onErrorCalls.get();
			r.r2 = stressSubscriber.droppedErrors.size();
		}
	}

	@JCStressTest
	@Outcome(id = {"1, 2, 0"}, expect = ACCEPTABLE, desc = "onError happened, bot error are as causes, zero dropped")
	@Outcome(id = {"1, 1, 1"}, expect = ACCEPTABLE, desc = "onError happened, only one error appeared and another was dropped")
	@State
	public static class MainErrorInnerErrorStressTest extends FluxSwitchMapStressTest {

		final Throwable t1 = new RuntimeException("test1");
		final Throwable t2 = new RuntimeException("test2");

		StressSubscription<Integer> subscription;

		{
			switchMapMain.onSubscribe(stressSubscription);
			switchMapMain.onNext("1");
			switchMapMain.request(1);
		}

		@Override
		Publisher<Object> handle(Object value) {
			return s -> {
				final StressSubscription subscription =
						new StressSubscription<>((CoreSubscriber) s);
				this.subscription = subscription;
				s.onSubscribe(subscription);
			};
		}

		@Actor
		public void outerProducer() {
			switchMapMain.onError(t1);
		}

		@Actor
		public void innerProducer() {
			subscription.actual.onNext(1);
			subscription.actual.onError(t2);
		}

		@Arbiter
		public void arbiter(III_Result r) {
			Hooks.resetOnErrorDropped();
			r.r1 = stressSubscriber.onErrorCalls.get();
			r.r2 = Exceptions.isMultiple(stressSubscriber.error)
					? stressSubscriber.error.getSuppressed().length
					: 1;
			r.r3 = stressSubscriber.droppedErrors.size();
		}
	}

	@JCStressTest
	@Outcome(id = {"200, 1"}, expect = ACCEPTABLE, desc = "Should produced and remaining requested result in total requested number of elements")
	@State
	public static class RequestAndProduceStressTest1 extends FluxSwitchMapStressTest {

		final TestPublisher<Object> testPublisher =
				TestPublisher.createColdNonCompliant(false, TestPublisher.Violation.CLEANUP_ON_TERMINATE, TestPublisher.Violation.DEFER_CANCELLATION);

		{
			switchMapMain.onSubscribe(stressSubscription);
		}

		@Override
		Publisher<Object> handle(Object value) {
			return testPublisher;
		}

		@Actor
		public void outerProducer() {
			for (int i = 0; i < 10; i++) {
				switchMapMain.onNext(i);
			}
			switchMapMain.onComplete();
		}

		@Actor
		public void innerProducer() {
			for (int i = 0; i < 20; i++) {
				testPublisher.next(i);
			}
			testPublisher.complete();
		}

		@Actor
		public void outerRequest() {
			for (int i = 0; i < 200; i++) {
				switchMapMain.request(1);
			}
		}

		@Arbiter
		public void arbiter(II_Result r) {
			r.r1 = (int) (stressSubscriber.onNextCalls.get() + switchMapMain.requested);
			r.r2 = stressSubscriber.onCompleteCalls.get();
		}
	}

	// Ignore, flaky test (https://github.com/reactor/reactor-core/issues/3633)
	//@JCStressTest
	@Outcome(id = {"200, 0, 0", "200, 0, 1"}, expect = ACCEPTABLE, desc = "Should " +
			"produced exactly what was requested")
	@State
	public static class RequestAndProduceStressTest2 extends FluxSwitchMapStressTest {

		final TestPublisher<Object> testPublisher =
				TestPublisher.createColdNonCompliant(false, TestPublisher.Violation.CLEANUP_ON_TERMINATE, TestPublisher.Violation.DEFER_CANCELLATION);

		{
			switchMapMain.onSubscribe(stressSubscription);
		}

		@Override
		Publisher<Object> handle(Object value) {
			return testPublisher;
		}

		@Actor
		public void outerProducer() {
			for (int i = 0; i < 10; i++) {
				switchMapMain.onNext(i);
			}
			switchMapMain.onComplete();
		}

		@Actor
		public void innerProducer() {
			for (int i = 0; i < 200; i++) {
				testPublisher.next(i);
			}
			testPublisher.complete();
		}

		@Actor
		public void outerRequest() {
			for (int i = 0; i < 200; i++) {
				switchMapMain.request(1);
			}
		}

		@Arbiter
		public void arbiter(III_Result r) {
			r.r1 = stressSubscriber.onNextCalls.get();
			r.r2 = (int) switchMapMain.requested;
			r.r3 = stressSubscriber.onCompleteCalls.get();

			switch (r.toString()) {
				case "200, 0, 0":
				case "200, 0, 1":
					break;
				default: throw new IllegalStateException(r + " " + fastLogger);
			}

			if (stressSubscriber.onNextCalls.get() < 200 && stressSubscriber.onNextDiscarded.get() < switchMapMain.requested) {
				throw new IllegalStateException(r + " " + fastLogger);
			}
		}
	}

	@JCStressTest
	@Outcome(id = {"200, 1"}, expect = ACCEPTABLE, desc = "Should produced and remaining requested result in total requested number of elements")
	@State
	public static class RequestAndProduceStressTest3 extends FluxSwitchMapStressTest {

		{
			switchMapMain.onSubscribe(stressSubscription);
		}

		@Override
		@SuppressWarnings({"rawtypes", "unchecked"})
		Publisher<Object> handle(Object value) {
			return (Publisher) Flux.range(((int) value) * 100, 20);
		}

		@Actor
		public void outerProducer() {
			for (int i = 0; i < 10; i++) {
				switchMapMain.onNext(i);
			}
			switchMapMain.onComplete();
		}

		@Actor
		public void outerRequest() {
			for (int i = 0; i < 200; i++) {
				switchMapMain.request(1);
			}
		}

		@Arbiter
		public void arbiter(JI_Result r) {
			r.r1 = stressSubscriber.onNextCalls.get() + switchMapMain.requested;
			r.r2 = stressSubscriber.onCompleteCalls.get();

			if (r.r1 < 200 || r.r2 == 0) {
				throw new IllegalStateException(r + " " + fastLogger);
			}
		}
	}

	@JCStressTest
	@Outcome(id = {"200, 0, 0", "200, 1, 0"}, expect = ACCEPTABLE, desc = "Should produced exactly what was requested")
	@State
	public static class RequestAndProduceStressTest4 extends FluxSwitchMapStressTest {

		{
			switchMapMain.onSubscribe(stressSubscription);
		}

		@Override
		@SuppressWarnings({"rawtypes", "unchecked"})
		Publisher<Object> handle(Object value) {
			return (Publisher) Flux.range(((int) value) * 1000, 200);
		}

		@Actor
		public void outerProducer() {
			for (int i = 0; i < 10; i++) {
				switchMapMain.onNext(i);
			}
			switchMapMain.onComplete();
		}

		@Actor
		public void outerRequest() {
			for (int i = 0; i < 200; i++) {
				switchMapMain.request(1);
			}
		}

		@Arbiter
		public void arbiter(IIL_Result r) {
			r.r1 = stressSubscriber.onNextCalls.get();
			r.r2 = stressSubscriber.onCompleteCalls.get();
			r.r3 = switchMapMain.requested;
		}
	}

	@JCStressTest
	//    onNextCalls
	//              |  onNextDiscarded
	//              |  |  onCompleteCalls
	//              |  |  |  requestedInner1
	//              |  |  |  |  requestedInner2
	//              |  |  |  |  |  remainingRequestedMain
	//              |  |  |  |  |  |
	@Outcome(id = {"1, 0, 0, 1, 1, 1, 4294967314"}, expect = ACCEPTABLE, desc = "inner1.onNext(0) -> main.onNext(1) -> main.request(1) -> inner2.onSubscribe" +
																				" || " +
																				"inner1.onNext(0) -> main.onNext(1) -> inner2.onSubscribe -> inner2.request(1)")
	@Outcome(id = {"1, 0, 0, 2, 1, 1, 4294967314"}, expect = ACCEPTABLE, desc = "some " +
			"extra case when onNext observed added value to REQUESTED -> unsetWip -> " +
			"subscribed to next inner and only after that slow addRequest added extra " +
			"value to hasRequest so it ends up with 2 increments")
	@Outcome(id = {"1, 0, 0, 1, 1, 1, 4294967330"}, expect = ACCEPTABLE)
	@Outcome(id = {"1, 0, 0, 2, 1, 1, 4294967330"}, expect = ACCEPTABLE, desc = "inner1.onNext(0) -> inner1.request(1) -> main.onNext(1) -> inner2.onSubscribe(request(1))")
	@Outcome(id = {"0, 1, 0, 1, 2, 2, 4294967314"}, expect = ACCEPTABLE, desc = "main.onNext(1) -> inner1.discarded(0) -> main.request(1) -> inner2.onSubscribe")
	@Outcome(id = {"0, 1, 0, 1, 1, 2, 4294967330"}, expect = ACCEPTABLE, desc = "main.onNext(1) -> inner1.discarded(0) -> inner2.onSubscribe -> main.request(1)")
	@Outcome(id = {"0, 1, 0, 1, 1, 2, 4294967330"}, expect = ACCEPTABLE, desc = "main.onNext(1) -> inner1.discarded(0) -> inner2.onSubscribe -> main.request(1)")
	@Outcome(id = {"0, 1, 0, 1, 2, 2, 4294967330"}, expect = ACCEPTABLE, desc = "main.onNext(1) -> inner1.discarded(0) -> inner2.onSubscribe(request(2)) | main.request(1)")
	@State
	public static class SimpleRequestAndProduceStressTest extends FluxSwitchMapStressTest {

		StressSubscription<Integer> subscription1;
		StressSubscription<Integer> subscription2;

		{
			switchMapMain.onSubscribe(stressSubscription);
			switchMapMain.request(1);
			switchMapMain.onNext(0);
		}

		@Override
		@SuppressWarnings("rawtypes")
		Publisher<Object> handle(Object value) {
			if ((int) value == 0) {
				return s -> {
					final StressSubscription subscription1 =
							new StressSubscription<>((CoreSubscriber) s);
					this.subscription1 = subscription1;
					s.onSubscribe(subscription1);
				};
			} else {
				return s -> {
					final StressSubscription subscription2 =
							new StressSubscription<>((CoreSubscriber) s);
					this.subscription2 = subscription2;
					s.onSubscribe(subscription2);
				};
			}
		}

		@Actor
		public void outerProducer() {
			switchMapMain.onNext(1);
		}

		@Actor
		public void innerProducer() {
			subscription1.actual.onNext(0);
		}

		@Actor
		public void outerRequest() {
			switchMapMain.request(1);
		}

		@Arbiter
		public void arbiter(JJJJJJJ_Result r) {
			r.r1 = stressSubscriber.onNextCalls.get();
			r.r2 = stressSubscriber.onNextDiscarded.get();
			r.r3 = stressSubscriber.onCompleteCalls.get();
			r.r4 = subscription1.requested;
			r.r5 = subscription2.requested;
			r.r6 = switchMapMain.requested;
			r.r7 = switchMapMain.state;
		}
	}
}