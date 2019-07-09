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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.publisher.TestPublisher;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

public class FluxOnBackpressureBufferTest
		extends FluxOperatorTest<String, String> {

	@Test
	public void bufferOverflowErrorDelayed() {
		TestPublisher<String> tp1 = TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);
		TestPublisher<String> tp2 = TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

		final Flux<String> test1 = tp1.flux().onBackpressureBuffer(3);
		final Flux<String> test2 = tp2.flux().onBackpressureBuffer(3, s -> { });

		StepVerifier.create(test1, StepVerifierOptions.create()
		                                              .scenarioName("without consumer")
		                                              .initialRequest(0))
		            .expectSubscription()
		            .then(() -> tp1.next("A", "B", "C", "D"))
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenRequest(3)
		            .expectNext("A", "B", "C")
		            .expectErrorMatches(Exceptions::isOverflow)
		            .verify(Duration.ofSeconds(5));

		StepVerifier.create(test2, StepVerifierOptions.create()
		                                             .scenarioName("with consumer")
		                                             .initialRequest(0))
		            .expectSubscription()
		            .then(() -> tp2.next("A", "B", "C", "D"))
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenRequest(3)
		            .expectNext("A", "B", "C")
		            .expectErrorMatches(Exceptions::isOverflow)
		            .verify(Duration.ofSeconds(5));
	}

	@Test(expected = IllegalArgumentException.class)
	public void failNegativeHistory(){
		Flux.never().onBackpressureBuffer(-1);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(Flux::onBackpressureBuffer),

				scenario(f -> f.onBackpressureBuffer(4, d -> {}))
		);
	}

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.fusionMode(Fuseable.ASYNC)
		                     .fusionModeThreadBarrier(Fuseable.ASYNC)
		                     .prefetch(Integer.MAX_VALUE);
	}

	@Test
	public void onBackpressureBuffer() {
		StepVerifier.create(Flux.range(1, 100)
		                        .onBackpressureBuffer(), 0)
		            .thenRequest(5)
		            .expectNext(1, 2, 3, 4, 5)
		            .thenAwait()
		            .thenRequest(90)
		            .expectNextCount(90)
		            .thenAwait()
		            .thenRequest(5)
		            .expectNext(96, 97, 98, 99, 100)
		            .verifyComplete();
	}

	@Test
	public void onBackpressureBufferMax() {
		StepVerifier.create(Flux.range(1, 100)
		                        .hide()
		                        .onBackpressureBuffer(8), 0)
		            .thenAwait() //be sure to delay the first request enough that the buffer overflows
		            .thenRequest(9)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8)
		            .verifyErrorMatches(Exceptions::isOverflow);
	}

	@Test
	public void onBackpressureBufferMaxCallback() {
		AtomicInteger last = new AtomicInteger();

		StepVerifier.create(Flux.range(1, 100)
		                        .hide()
		                        .onBackpressureBuffer(8, last::set), 0)

		            .thenAwait() //be sure to delay the first request enough that the buffer overflows
		            .thenRequest(9)
		            .expectNext(1, 2, 3, 4, 5, 6, 7, 8)
		            .verifyErrorMatches(Exceptions::isOverflow);
	}

	//see https://github.com/reactor/reactor-core/issues/1666
	@Test
	public void onBackpressureBufferMaxCallbackUnder8() {
		CopyOnWriteArrayList<Integer> overflown = new CopyOnWriteArrayList<>();
		AtomicInteger producedCounter = new AtomicInteger();

		StepVerifier.create(Flux.range(1, 15)
		                        .doOnNext(i -> producedCounter.incrementAndGet())
		                        .hide()
		                        .onBackpressureBuffer(3, overflown::add),
				StepVerifierOptions.create().initialRequest(0).checkUnderRequesting(false))
		            .thenRequest(5)
		            .expectNext(1, 2, 3, 4, 5)
		            .thenAwait() //at this point the buffer is overrun since the range request was unbounded
		            .thenRequest(100) //requesting more empties the buffer before an overflow error is propagated
		            .expectNext(6, 7, 8)
		            .expectErrorMatches(Exceptions::isOverflow)
		            .verifyThenAssertThat()
		            .hasNotDroppedElements();

		assertThat(overflown).as("passed to overflow handler").containsExactly(9);
		assertThat(producedCounter).as("good source cancelled on first overflow").hasValue(9);
	}

	//see https://github.com/reactor/reactor-core/issues/1666
	@Test
	public void onBackpressureBufferMaxCallbackSourceEmitsAfterComplete() {
		TestPublisher<Integer> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION);
		CopyOnWriteArrayList<Integer> overflown = new CopyOnWriteArrayList<>();
		AtomicInteger producedCounter = new AtomicInteger();

		StepVerifier.create(testPublisher.flux()
		                                 .doOnNext(i -> producedCounter.incrementAndGet())
		                                 .onBackpressureBuffer(3, overflown::add),
				StepVerifierOptions.create().initialRequest(0).checkUnderRequesting(false))
		            .thenRequest(5)
					.then(() -> testPublisher.next(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15))
		            .expectNext(1, 2, 3, 4, 5)
		            .thenAwait() //at this point the buffer is overrun since the range request was unbounded
		            .thenRequest(100) //requesting more empties the buffer before an overflow error is propagated
		            .expectNext(6, 7, 8)
		            .expectErrorMatches(Exceptions::isOverflow)
		            .verifyThenAssertThat()
		            .hasDroppedExactly(10, 11, 12, 13, 14, 15);

		//the rest, asserted above, is dropped because the source was cancelled
		assertThat(overflown).as("passed to overflow handler").containsExactly(9);
		assertThat(producedCounter).as("bad source produced").hasValue(15);
	}

	//see https://github.com/reactor/reactor-core/issues/1666
	@Test
	public void stepByStepRequesting() {
		List<Long> discardedItems = Collections.synchronizedList(new ArrayList<>());
		AtomicInteger sourceProduced = new AtomicInteger();
		AtomicInteger sourceCancelledAt = new AtomicInteger(-1);

		StepVerifier.withVirtualTime(() ->
						Flux.interval(Duration.ofSeconds(1)) // lets emit 1 item per second; starting with zero
						    .take(8)
						    .doOnNext(v -> sourceProduced.incrementAndGet())
						    .doOnCancel(() -> sourceCancelledAt.set(sourceProduced.get() - 1))
						    .onBackpressureBuffer(2, discardedItems::add)
				, 0)
		            .expectSubscription()
		            .thenAwait(Duration.ofSeconds(1)) // request is in sync with producer for now
		            //stores 0: (0)
		            .thenRequest(1)
		            //drains 0: ()
		            .expectNext(0L)
		            .thenAwait(Duration.ofSeconds(2)) // request is behind producer by 1
		            //stores 1 and 2: (1, 2)
		            .thenRequest(1)
		            //drains 1: (2)
		            .expectNext(1L)
		            .thenAwait(Duration.ofSeconds(2))//request has now been behind producer two times
		            //stores 3: (2, 3), attempts to store 4 => overflows 4, cancels
		            .then(() -> Assertions.assertThat(sourceCancelledAt).as("source cancelled at").hasValue(4))
		            .thenRequest(1)
		            //drains 2: (3)
		            .expectNext(2L)
		            .thenAwait(Duration.ofSeconds(4)) //this shouldn't produce any new elements as source cancelled
		            .thenRequest(1)
		            //finally drains buffer: (), propagate overflow
		            .expectNext(3L)
		            .verifyErrorMatches(Exceptions::isOverflow);

		Assertions.assertThat(discardedItems).containsExactly(4L);
		Assertions.assertThat(sourceProduced).as("source produced").hasValue(5);
	}

	@Test
	public void gh1666_bufferThree() {
		AtomicInteger overflow = new AtomicInteger(-1);
		StepVerifier.create(Flux.range(1, 100)
		                        .hide()
		                        .onBackpressureBuffer(3, overflow::set)
				, 0)
		            .expectSubscription()
		            .thenRequest(1)
		            .expectNext(1)
		            .thenAwait()
		            .thenCancel()
		            .verify();

		assertThat(overflow).as("overflow value").hasValue(5);
	}

	@Test
	public void gh1666_bufferNine() {
		AtomicInteger overflow = new AtomicInteger();
		StepVerifier.create(Flux.range(1, 100)
		                        .hide()
		                        .onBackpressureBuffer(9, overflow::set)
				, 0)
		            .expectSubscription()
		            .thenRequest(1)
		            .expectNext(1)
		            .thenAwait()
		            .thenCancel()
		            .verify();

		assertThat(overflow).as("overflow value").hasValue(11);
	}

	@Test
    public void scanSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxOnBackpressureBuffer.BackpressureBufferSubscriber<Integer> test =
        		new FluxOnBackpressureBuffer.BackpressureBufferSubscriber<>(actual,
        				123, false, t -> {});
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
        test.requested = 35;
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(0); // RS: TODO non-zero size

        assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

    @Test
	public void scanCapacityUnbounded() {
	    CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
	    FluxOnBackpressureBuffer.BackpressureBufferSubscriber<Integer> test =
			    new FluxOnBackpressureBuffer.BackpressureBufferSubscriber<>(actual,
					    123, true, t -> {});

	    assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
	public void scanCapacityBoundedQueueWithExactCapacity() {
		int exactCapacity = 16;

	    CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
	    FluxOnBackpressureBuffer.BackpressureBufferSubscriber<Integer> test =
			    new FluxOnBackpressureBuffer.BackpressureBufferSubscriber<>(actual,
					    exactCapacity, false, t -> {});

	    assumeThat(Queues.capacity(test.queue)).as("Queue has exact required capacity").isEqualTo(exactCapacity);

	    assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(exactCapacity);
    }

    @Test
	public void scanCapacityBoundedQueueWithExtraCapacity() {
		int desiredCapacity = 12;

	    CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
	    FluxOnBackpressureBuffer.BackpressureBufferSubscriber<Integer> test =
			    new FluxOnBackpressureBuffer.BackpressureBufferSubscriber<>(actual,
					    desiredCapacity, false, t -> {});

	    assumeThat(Queues.capacity(test.queue)).as("Queue has greater than required capacity").isGreaterThan(desiredCapacity);

	    assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(desiredCapacity);
    }
}