/*
 * Copyright (c) 2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.LLL_Result;
import org.openjdk.jcstress.infra.results.LL_Result;
import reactor.core.util.FastLogger;
import reactor.test.scheduler.VirtualTimeScheduler;

import static java.util.Collections.emptyList;

public class FluxBufferTimeoutStressTest {

	@JCStressTest
	@Outcome(id = "1, 1", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "2, 1", expect = Expect.ACCEPTABLE, desc = "")
	@State
	public static class FluxBufferTimeoutStressTestRaceDeliveryAndTimeout {

		final FastLogger fastLogger = new FastLogger(getClass().getName());

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();

		final StressSubscriber<List<Long>> subscriber = new StressSubscriber<>();

		final FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<Long, List<Long>> bufferTimeoutSubscriber =
				new FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<>(subscriber, 2, 1, TimeUnit.SECONDS, virtualTimeScheduler.createWorker(), bufferSupplier(), fastLogger);

		final StressSubscription<Long> subscription = new StressSubscription<>(bufferTimeoutSubscriber);

		{
			bufferTimeoutSubscriber.onSubscribe(subscription);
		}

		@Actor
		public void next() {
			bufferTimeoutSubscriber.onNext(0L);
			bufferTimeoutSubscriber.onNext(1L);
			bufferTimeoutSubscriber.onComplete();
		}

		@Actor
		public void timeout() {
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
		}

		@Arbiter
		public void arbiter(LL_Result result) {
			result.r1 = subscriber.onNextCalls.get();
			result.r2 = subscription.requestsCount.get();

			if (subscriber.onCompleteCalls.get() != 1) {
				fail(fastLogger,
						"unexpected completion count " + subscriber.onCompleteCalls.get());
			}
			if (subscriber.concurrentOnComplete.get()) {
				fail(fastLogger, "subscriber concurrent onComplete");
			}
			if (subscriber.concurrentOnNext.get()) {
				fail(fastLogger, "subscriber concurrent onNext");
			}
			if (!subscriber.discardedValues.isEmpty()) {
				fail(fastLogger, "Unexpected discarded values " + subscriber.discardedValues);
			}
		}
	}

	@JCStressTest
	@Outcome(id = "3, 1", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "4, 1", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "5, 1", expect = Expect.ACCEPTABLE, desc = "")
	@State
	public static class FluxBufferTimeoutStressTestRaceDeliveryAndMoreTimeouts {

		final FastLogger fastLogger = new FastLogger(getClass().getName());

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();

		final StressSubscriber<List<Long>> subscriber = new StressSubscriber<>();

		final FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<Long, List<Long>> bufferTimeoutSubscriber =
				new FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<>(subscriber, 2, 1, TimeUnit.SECONDS, virtualTimeScheduler.createWorker(), bufferSupplier(), fastLogger);

		final StressSubscription<Long> subscription = new StressSubscription<>(bufferTimeoutSubscriber);

		{
			bufferTimeoutSubscriber.onSubscribe(subscription);
		}

		@Actor
		public void next() {
			bufferTimeoutSubscriber.onNext(0L);
			bufferTimeoutSubscriber.onNext(1L);
			bufferTimeoutSubscriber.onNext(2L);
			bufferTimeoutSubscriber.onNext(3L);
			bufferTimeoutSubscriber.onNext(4L);

			bufferTimeoutSubscriber.onComplete();
		}

		@Actor
		public void timeout() {
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
		}

		@Arbiter
		public void arbiter(LL_Result result) {
			result.r1 = subscriber.onNextCalls.get();
			result.r2 = subscription.requestsCount.get();

			if (subscriber.onCompleteCalls.get() != 1) {
				fail(fastLogger, "unexpected completion: " + subscriber.onCompleteCalls.get());
			}
			if (subscriber.concurrentOnComplete.get()) {
				fail(fastLogger, "subscriber concurrent onComplete");
			}
			if (subscriber.concurrentOnNext.get()) {
				fail(fastLogger, "subscriber concurrent onNext");
			}
			if (!subscriber.discardedValues.isEmpty()) {
				fail(fastLogger, "Unexpected discarded values " + subscriber.discardedValues);
			}
			if (!allValuesHandled(fastLogger, 5, emptyList(),
					subscriber.receivedValues)) {
				fail(fastLogger, "not all values delivered; result=" + result);
			}
		}
	}

	@JCStressTest
	@Outcome(id = "5, 1, 2", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "5, 1, 3", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "5, 1, 4", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "5, 1, 5", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "5, 0, 2", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "5, 0, 3", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "5, 0, 4", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "5, 0, 5", expect = Expect.ACCEPTABLE, desc = "")
	@State
	public static class FluxBufferTimeoutStressTestRaceDeliveryAndMoreTimeoutsPossiblyIncomplete {

		final FastLogger fastLogger = new FastLogger(getClass().getName());

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();

		final StressSubscriber<List<Long>> subscriber = new StressSubscriber<>(1);

		final FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<Long, List<Long>> bufferTimeoutSubscriber =
				new FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<>(subscriber, 2, 1, TimeUnit.SECONDS, virtualTimeScheduler.createWorker(), bufferSupplier(), fastLogger);

		Sinks.Many<Long> proxy = Sinks.unsafe().many().unicast().onBackpressureBuffer();

		final AtomicLong requested = new AtomicLong();

		{
			proxy.asFlux()
			     .doOnRequest(r -> requested.incrementAndGet())
			     .subscribe(bufferTimeoutSubscriber);
		}

		@Actor
		public void next() {
			proxy.tryEmitNext(0L);
			proxy.tryEmitNext(1L);
			proxy.tryEmitNext(2L);
			proxy.tryEmitNext(3L);
			proxy.tryEmitNext(4L);

			proxy.tryEmitNext(5L);
			proxy.tryEmitNext(6L);
			proxy.tryEmitNext(7L);
			proxy.tryEmitNext(8L);
			proxy.tryEmitNext(9L);
			proxy.tryEmitComplete();
		}

		@Actor
		public void timeout() {
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
		}

		@Actor
		public void request() {
			subscriber.request(1);
			subscriber.request(1);
			subscriber.request(1);
			subscriber.request(1);
		}

		@Arbiter
		public void arbiter(LLL_Result result) {
			result.r1 = subscriber.onNextCalls.get();
			result.r2 = subscriber.onCompleteCalls.get();
			result.r3 = requested.get();

			if (!allValuesHandled(fastLogger, 4, emptyList(), subscriber.receivedValues)) {
				fail(fastLogger, "minimum set of values not delivered");
			}

			if (subscriber.onCompleteCalls.get() == 0) {
				if (subscriber.receivedValues.size() == 5 &&
						subscriber.receivedValues.stream().noneMatch(buf -> buf.size() == 1)) {
					fail(fastLogger, "incomplete but delivered all two " +
							"element buffers. received: " + subscriber.receivedValues + "; result=" + result);
				}
			}

			// TODO #onNext < 5 and incomplete => why fail?
			if (subscriber.onNextCalls.get() < 5 && subscriber.onCompleteCalls.get() == 0) {
				fail(fastLogger, "incomplete. received: " + subscriber.receivedValues + "; requested=" + requested.get() + "; result=" + result);
			}

			if (subscriber.onCompleteCalls.get() > 1) {
				fail(fastLogger, "unexpected completion " + subscriber.onCompleteCalls.get());
			}
			if (subscriber.concurrentOnComplete.get()) {
				fail(fastLogger, "subscriber concurrent onComplete");
			}
			if (subscriber.concurrentOnNext.get()) {
				fail(fastLogger, "subscriber concurrent onNext");
			}
			if (subscriber.receivedValues.stream().anyMatch(List::isEmpty)) {
				fail(fastLogger, "received an empty buffer: " + subscriber.receivedValues + "; result=" + result);
			}
		}
	}

	@JCStressTest
	@Outcome(id = "1, 1, 1", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "0, 0, 1", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "1, 0, 1", expect = Expect.ACCEPTABLE, desc = "")
	@State
	public static class FluxBufferTimeoutStressTestRaceDeliveryAndCancel {

		final FastLogger fastLogger = new FastLogger(getClass().getName());

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();

		final StressSubscriber<List<Long>> subscriber = new StressSubscriber<>();

		final FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<Long, List<Long>> bufferTimeoutSubscriber =
				new FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<>(subscriber, 2, 1, TimeUnit.SECONDS, virtualTimeScheduler.createWorker(), bufferSupplier(), fastLogger);

		final StressSubscription<Long> subscription = new StressSubscription<>(bufferTimeoutSubscriber);

		{
			bufferTimeoutSubscriber.onSubscribe(subscription);
		}

		@Actor
		public void next() {
			bufferTimeoutSubscriber.onNext(0L);
			bufferTimeoutSubscriber.onNext(1L);
			bufferTimeoutSubscriber.onComplete();
		}

		@Actor
		public void cancel() {
			bufferTimeoutSubscriber.cancel();
		}

		@Arbiter
		public void arbiter(LLL_Result result) {
			result.r1 = subscriber.onNextCalls.get();
			result.r2 = subscriber.onCompleteCalls.get();
			result.r3 = subscription.requestsCount.get();

			if (subscriber.onCompleteCalls.get() > 1) {
				fail(fastLogger, "unexpected completion " + subscriber.onCompleteCalls.get());
			}
			if (subscriber.concurrentOnComplete.get()) {
				fail(fastLogger, "subscriber concurrent onComplete");
			}
			if (subscriber.concurrentOnNext.get()) {
				fail(fastLogger, "subscriber concurrent onNext");
			}
			if (!allValuesHandled(fastLogger, 2, subscriber.discardedValues, subscriber.receivedValues)) {
				fail(fastLogger, "Not all handled!" + "; result=" + result);
			}
		}
	}

	@JCStressTest
	@Outcome(id = "0, 0, 1", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "1, 0, 1", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "1, 0, 2", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "2, 0, 1", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "2, 0, 2", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "2, 1, 1", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "2, 1, 2", expect = Expect.ACCEPTABLE, desc = "")
	@State
	public static class FluxBufferTimeoutStressTestRaceDeliveryAndCancelWithBackpressure {

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();

		final StressSubscriber<List<Long>> subscriber = new StressSubscriber<>(1);

		final FastLogger fastLogger = new FastLogger(getClass().getName());
		final FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<Long, List<Long>> bufferTimeoutSubscriber =
				new FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<>(subscriber, 2, 1, TimeUnit.SECONDS, virtualTimeScheduler.createWorker(), bufferSupplier(), fastLogger);

		Sinks.Many<Long> proxy = Sinks.unsafe().many().unicast().onBackpressureBuffer();
		AtomicLong emits = new AtomicLong();
		final AtomicLong requested = new AtomicLong();
		{
			proxy.asFlux()
			     .doOnRequest(r -> requested.incrementAndGet())
			     .subscribe(bufferTimeoutSubscriber);
		}

		@Actor
		public void next() {
			for (long i = 0; i < 4; i++) {
				if (proxy.tryEmitNext(i) != Sinks.EmitResult.OK) {
					return;
				}
				emits.set(i + 1);
			}
			proxy.tryEmitComplete();
		}

		@Actor
		public void request() {
			subscriber.request(1);
		}

		@Actor
		public void cancel() {
			subscriber.cancel();
		}

		@Arbiter
		public void arbiter(LLL_Result result) {
			result.r1 = subscriber.onNextCalls.get();
			result.r2 = subscriber.onCompleteCalls.get();
			result.r3 = requested.get();

			if (subscriber.onCompleteCalls.get() > 1) {
				fail(fastLogger, "unexpected completion " + subscriber.onCompleteCalls.get());
			}
			if (subscriber.concurrentOnComplete.get()) {
				fail(fastLogger, "subscriber concurrent onComplete");
			}
			if (subscriber.concurrentOnNext.get()) {
				fail(fastLogger, "subscriber concurrent onNext");
			}
			int emits = (int) this.emits.get();
			if (subscriber.onCompleteCalls.get() == 1 && !allValuesHandled(fastLogger, 4,
					emptyList(),
					subscriber.receivedValues)) {
				fail(fastLogger,
						"Completed but not all values handled!" + "; result=" + result);
			}

			if (subscriber.onNextCalls.get() > 0 && !allValuesHandled(fastLogger, emits,
					subscriber.discardedValues,
					subscriber.receivedValues)) {
				fail(fastLogger, "Not all " + emits + " emits handled!" + "; result=" + result);
			}
		}
	}

	@JCStressTest
	@Outcome(id = "1, 1, 1", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "2, 1, 1", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "0, 0, 1", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "1, 0, 1", expect = Expect.ACCEPTABLE, desc = "")
	@Outcome(id = "2, 0, 1", expect = Expect.ACCEPTABLE, desc = "")
	@State
	public static class FluxBufferTimeoutStressTestRaceDeliveryAndCancelAndTimeout {

		final FastLogger fastLogger = new FastLogger(getClass().getName());

		final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.create();

		final StressSubscriber<List<Long>> subscriber = new StressSubscriber<>();

		final FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<Long, List<Long>> bufferTimeoutSubscriber =
				new FluxBufferTimeout.BufferTimeoutWithBackpressureSubscriber<>(subscriber, 2, 1, TimeUnit.SECONDS, virtualTimeScheduler.createWorker(), bufferSupplier(), fastLogger);

		final StressSubscription<Long> subscription = new StressSubscription<>(bufferTimeoutSubscriber);

		{
			bufferTimeoutSubscriber.onSubscribe(subscription);
		}

		@Actor
		public void next() {
			bufferTimeoutSubscriber.onNext(0L);
			bufferTimeoutSubscriber.onNext(1L);
			bufferTimeoutSubscriber.onComplete();
		}

		@Actor
		public void cancel() {
			bufferTimeoutSubscriber.cancel();
		}

		@Actor
		public void timeout() {
			virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(1));
		}

		@Arbiter
		public void arbiter(LLL_Result result) {
			result.r1 = subscriber.onNextCalls.get();
			result.r2 = subscriber.onCompleteCalls.get();
			result.r3 = subscription.requestsCount.get();

			if (subscriber.onCompleteCalls.get() > 1) {
				fail(fastLogger,
						"unexpected completion " + subscriber.onCompleteCalls.get());
			}
			if (subscriber.concurrentOnComplete.get()) {
				fail(fastLogger, "subscriber concurrent onComplete");
			}
			if (subscriber.concurrentOnNext.get()) {
				fail(fastLogger, "subscriber concurrent onNext");
			}

			if (!allValuesHandled(fastLogger, 2, subscriber.discardedValues, subscriber.receivedValues)) {
				fail(fastLogger, "Not all handled!" + "; result=" + result);
			}
		}
	}

	private static void fail(FastLogger fastLogger, String msg) {
		throw new IllegalStateException(msg + "\n" + fastLogger);
	}

	private static boolean allValuesHandled(FastLogger logger, int range, List<Object> discarded, List<List<Long>>... delivered) {
		if (delivered.length == 0) {
			return false;
		}

		List<Long> discardedValues = discarded.stream()
		                              .map(o -> (Long) o)
		                              .collect(Collectors.toList());

		logger.trace("discarded: " + discardedValues);
		logger.trace("delivered: " + Arrays.toString(delivered));

		boolean[] search = new boolean[range];
		for (long l : discardedValues) {
			search[(int) l] = true;
		}

		List<List<Long>> all =
				Arrays.stream(delivered)
				      .flatMap(lists -> lists.stream())
				      .collect(Collectors.toList());

		for (List<Long> buf : all) {
			if (buf.isEmpty()) {
				fail(logger, "Received empty buffer!");
			}
			for (long l : buf) {
				if (l >= range) {
					// just check within the range
					continue;
				}
				if (search[(int) l]) {
					fail(logger, "Duplicate value (both discarded " +
							"and delivered, or duplicated in multiple buffers)");
				}
				search[(int) l] = true;
			}
		}
		for (boolean b : search) {
			if (!b) {
				return false;
			}
		}
		return true;
	}
	private static Supplier<List<Long>> bufferSupplier() {
		return ArrayList::new;
	}
}
