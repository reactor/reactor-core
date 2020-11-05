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

package reactor.core.publisher;

import java.time.Duration;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable.Attr;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Scannable.Attr.RunStyle.SYNC;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

class FluxTimedTest {

	private static Runnable emit(String value, Duration delay, Sinks.Many<String> sink, VirtualTimeScheduler vts) {
		return () -> {
			vts.advanceTimeBy(delay);
			sink.emitNext(value, FAIL_FAST);
		};
	}

	@Test
	void compareTimedElapsedNanosToElapsedT1() {
		//we'll manipulate the vts and emit values manually in order to show what happens with half milliseconds
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		Sinks.Many<String> sink = Sinks.unsafe().many().multicast().directBestEffort();

		Consumer<? super Timed<Tuple2<Long, String>>> timedSimilarToElapsed = v -> {
			assertThat(v.elapsed().toMillis())
					.as(v.toString())
					//Duration.toMillis() drops the nanosecond part entirely
					.isEqualTo(123L)
					.isBetween(v.get().getT1() - 1L, v.get().getT1());
		};

		StepVerifier.withVirtualTime(() -> sink.asFlux().elapsed().timed(),
				() -> vts, Long.MAX_VALUE)
		            .then(emit("A", Duration.ofNanos(123 * 1_000_000), sink, vts))
		            .assertNext(timedSimilarToElapsed)
		            .then(emit("B", Duration.ofNanos(1234 * 100_000), sink, vts))
		            .assertNext(timedSimilarToElapsed)
		            .then(emit("C", Duration.ofNanos(1235 * 100_000), sink, vts))
		            .assertNext(timedSimilarToElapsed)
		            .then(emit("D", Duration.ofNanos(12355 * 10_000), sink, vts)) //this is the one that gets truncated down
		            .assertNext(timedSimilarToElapsed)
		            .then(() -> sink.tryEmitComplete().orThrow())
		            .verifyComplete();
	}

	@Test
	void compareTimedTimetampNanosToTimestampT1() {
		//we'll manipulate the vts and emit values manually in order to show what happens with half milliseconds
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		Sinks.Many<String> sink = Sinks.unsafe().many().multicast().directBestEffort();

		Consumer<? super Timed<Tuple2<Long, String>>> timedSimilarToTimestamp = v -> {
			assertThat(v.timestamp().toEpochMilli())
					.as(v.toString())
					.isLessThan(1000L)
					//Instant.toEpochMilli() drops the nanosecond part entirely
					.isBetween(v.get().getT1() -1L, v.get().getT1());
		};

		StepVerifier.withVirtualTime(() -> sink.asFlux().timestamp().timed(),
				() -> vts, Long.MAX_VALUE)
		            .then(emit("A", Duration.ofNanos(123 * 1_000_000), sink, vts))
		            .assertNext(timedSimilarToTimestamp)
		            .then(emit("B", Duration.ofNanos(1234 * 100_000), sink, vts))
		            .assertNext(timedSimilarToTimestamp)
		            .then(emit("C", Duration.ofNanos(1235 * 100_000), sink, vts))
		            .assertNext(timedSimilarToTimestamp)
		            .then(emit("D", Duration.ofNanos(12355 * 10_000), sink, vts)) //this is the one that gets truncated down
		            .assertNext(timedSimilarToTimestamp)
		            .then(() -> sink.tryEmitComplete().orThrow())
		            .verifyComplete();
	}

	@Test
	void timedSinceSubscription() {
		Sinks.Many<String> sink = Sinks.unsafe().many().multicast().directBestEffort();
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		AssertSubscriber<String> assertSubscriber = AssertSubscriber.create();

		sink.asFlux().timed(vts)
		    .map(timed -> timed.elapsed().toNanos() + "->" + timed.elapsedSinceSubscription().toNanos())
		    .subscribe(assertSubscriber);

		vts.advanceTimeBy(Duration.ofNanos(3));
		sink.tryEmitNext("A").orThrow(); //at 3ns
		vts.advanceTimeBy(Duration.ofNanos(12));
		sink.tryEmitNext("B").orThrow(); //at 15ns
		vts.advanceTimeBy(Duration.ofNanos(300));
		sink.tryEmitNext("C").orThrow(); //at 315ns
		sink.tryEmitComplete().orThrow();

		assertSubscriber.assertValues("3->3", "12->15", "300->315")
		                .assertComplete();
	}

	@Test
	void errorThenNext() {
		TestPublisher<String> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(testPublisher.flux().timed())
		            .then(() -> testPublisher
				            .error(new IllegalStateException("boom"))
				            .next("DROP"))
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDropped("DROP");
	}

	@Test
	void errorThenError() {
		TestPublisher<String> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(testPublisher.flux().timed())
		            .then(() -> testPublisher
				            .error(new IllegalStateException("boom"))
				            .error(new IllegalStateException("DROP")))
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDroppedErrorWithMessage("DROP");
	}

	@Test
	void completeThenNext() {
		TestPublisher<String> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		StepVerifier.create(testPublisher.flux().timed())
		            .then(() -> testPublisher
				            .complete()
				            .next("DROP"))
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDropped("DROP");
	}

	@Test
	void completeThenComplete() {
		TestPublisher<String> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		AssertSubscriber<Timed<String>> assertSubscriber = AssertSubscriber.create();

		testPublisher.flux().timed().subscribe(assertSubscriber);

		testPublisher.complete()
		             .complete();

		//this would detect 2 completions and throw
		assertSubscriber.assertComplete();
	}

	@Test
	void cancelIsPropagated() {
		TestPublisher<String> testPublisher = TestPublisher.create();
		AssertSubscriber<Timed<String>> assertSubscriber = AssertSubscriber.create();

		testPublisher.flux().timed().subscribe(assertSubscriber);

		assertSubscriber.cancel();

		assertSubscriber.assertNotTerminated();
		testPublisher.assertWasSubscribed();
		testPublisher.assertCancelled();
	}

	@Test
	void completeThenError() {
		TestPublisher<String> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);

		StepVerifier.create(testPublisher.flux().timed())
		            .then(() -> testPublisher
				            .complete()
				            .error(new IllegalStateException("DROP")))
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedErrorWithMessage("DROP");
	}

	@Test
	void scanOperator() {
		Flux<String> source = Flux.just("example");
		FluxTimed<String> operator = new FluxTimed<>(source, Schedulers.immediate());

		assertThat(operator.scan(Attr.RUN_STYLE)).as("RUN_STYLE").isSameAs(SYNC);
		assertThat(operator.scan(Attr.PREFETCH)).as("PREFETCH").isEqualTo(0);
		assertThat(operator.scan(Attr.PARENT)).as("PARENT").isSameAs(source);
	}

	@Test
	void scanInner() {
		final CoreSubscriber<Object> actual = AssertSubscriber.create();
		final FluxTimed.TimedSubscriber<String> subscriber = new FluxTimed.TimedSubscriber<>(actual, Schedulers.immediate());

		assertThat(subscriber.scan(Attr.RUN_STYLE)).as("RUN_STYLE").isSameAs(SYNC);

		assertThat(subscriber.scanUnsafe(Attr.ACTUAL)).as("ACTUAL").isSameAs(actual);

		assertThat(subscriber.scan(Attr.TERMINATED)).as("TERMINATED before").isFalse();
		subscriber.onComplete();
		assertThat(subscriber.scan(Attr.TERMINATED)).as("TERMINATED after onComplete").isTrue();

		subscriber.done = false; //reset
		assertThat(subscriber.scan(Attr.TERMINATED)).as("TERMINATED reset").isFalse();
		subscriber.onError(new IllegalStateException("boom"));
		assertThat(subscriber.scan(Attr.TERMINATED)).as("TERMINATED after onError").isTrue();
	}

}