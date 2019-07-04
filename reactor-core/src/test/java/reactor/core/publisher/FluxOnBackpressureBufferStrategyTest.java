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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static reactor.core.publisher.BufferOverflowStrategy.*;

public class FluxOnBackpressureBufferStrategyTest implements Consumer<String>,
                                                             BiFunction<Throwable, Object, Throwable> {

	private String droppedValue;
	private Object hookCapturedValue;
	private Throwable hookCapturedError;

	@Override
	public void accept(String s) {
		this.droppedValue = s;
	}

	@Override
	public Throwable apply(Throwable throwable, Object o) {
		this.hookCapturedValue = o;
		this.hookCapturedError = throwable;
		return throwable;
	}

	@Before
	public void before() {
		this.droppedValue = null;
		this.hookCapturedError = null;
		this.hookCapturedValue = null;
		Hooks.onOperatorError(this);
	}

	@After
	public void after() {
		Hooks.resetOnOperatorError();
	}

	@Test
	public void bufferOverflowOverflowDelayedWithErrorStrategy() {
		TestPublisher<String> tp1 = TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);
		TestPublisher<String> tp2 = TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

		final Flux<String> test1 = tp1.flux().onBackpressureBuffer(3, ERROR);
		final Flux<String> test2 = tp2.flux().onBackpressureBuffer(3, s -> { }, ERROR);

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

	@Test
	public void drop() {
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, this, DROP_LATEST);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over1", "over2")
		            .expectComplete()
		            .verify();

		assertEquals("over3", droppedValue);
		assertNull("unexpected hookCapturedValue", hookCapturedValue);
		assertNull("unexpected hookCapturedError",hookCapturedError);
	}

	@Test
	public void dropOldest() {
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, this, DROP_OLDEST);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over2", "over3")
		            .expectComplete()
		            .verify();

		assertEquals("over1", droppedValue);
		assertNull("unexpected hookCapturedValue",hookCapturedValue);
		assertNull("unexpected hookCapturedError",hookCapturedError);
	}

	@Test
	public void error() {
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, this, ERROR);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over1", "over2")
		            .expectErrorMessage("The receiver is overrun by more signals than expected (bounded queue...)")
		            .verify();

		assertEquals("over3", droppedValue);
		assertEquals("over3", hookCapturedValue);
		assertTrue("unexpected hookCapturedError: " + hookCapturedError, hookCapturedError instanceof IllegalStateException);
	}

	//the 3 onBackpressureBufferMaxCallbackOverflow are similar to the tests above, except they use the public API
	@Test
	public void onBackpressureBufferMaxCallbackOverflowError() {
		AtomicInteger last = new AtomicInteger();

		StepVerifier.create(Flux.range(1, 100)
		                        .hide()
		                        .onBackpressureBuffer(8, last::set, BufferOverflowStrategy.ERROR), 0)

		            .thenRequest(7)
		            .expectNext(1, 2, 3, 4, 5, 6, 7)
		            .then(() -> assertThat(last.get()).isEqualTo(16))
		            .thenRequest(9)
		            .expectNextCount(8)
		            .verifyErrorMatches(Exceptions::isOverflow);
	}

	@Test
	public void onBackpressureBufferMaxCallbackOverflowDropOldest() {
		AtomicInteger last = new AtomicInteger();

		StepVerifier.create(Flux.range(1, 100)
		                        .hide()
		                        .onBackpressureBuffer(8, last::set,
				                        BufferOverflowStrategy.DROP_OLDEST), 0)

		            .thenRequest(7)
		            .expectNext(1, 2, 3, 4, 5, 6, 7)
		            .then(() -> assertThat(last.get()).isEqualTo(92))
		            .thenRequest(9)
		            .expectNext(93, 94, 95, 96, 97, 98, 99, 100)
		            .verifyComplete();
	}

	@Test
	public void onBackpressureBufferMaxCallbackOverflowDropLatest() {
		AtomicInteger last = new AtomicInteger();

		StepVerifier.create(Flux.range(1, 100)
		                        .hide()
		                        .onBackpressureBuffer(8, last::set,
				                        BufferOverflowStrategy.DROP_LATEST), 0)

		            .thenRequest(7)
		            .expectNext(1, 2, 3, 4, 5, 6, 7)
		            .then(() -> assertThat(last.get()).isEqualTo(100))
		            .thenRequest(9)
		            .expectNext(8, 9, 10, 11, 12, 13, 14, 15)
		            .verifyComplete();
	}


	@Test
	public void onBackpressureBufferWithBadSourceEmitsAfterComplete() {
		TestPublisher<Integer> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION);
		CopyOnWriteArrayList<Integer> overflown = new CopyOnWriteArrayList<>();
		AtomicInteger producedCounter = new AtomicInteger();

		StepVerifier.create(testPublisher.flux()
		                                 .doOnNext(i -> producedCounter.incrementAndGet())
		                                 .onBackpressureBuffer(3, overflown::add, BufferOverflowStrategy.ERROR),
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

	@Test
	public void dropCallbackError() {
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, v -> { throw new IllegalArgumentException("boom"); },
				DROP_LATEST);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over1", "over2")
		            .expectErrorMessage("boom")
		            .verify();

		assertNull("unexpected droppedValue", droppedValue);
		assertEquals("over3", hookCapturedValue);
		assertTrue("unexpected hookCapturedError: " + hookCapturedError, hookCapturedError instanceof IllegalArgumentException);
		assertEquals("boom", hookCapturedError.getMessage());
	}

	@Test
	public void dropOldestCallbackError() {
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, v -> { throw new IllegalArgumentException("boom"); },
				DROP_OLDEST);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over2", "over3")
		            .expectErrorMessage("boom")
		            .verify();

		assertNull("unexpected droppedValue", droppedValue);
		assertEquals("over1", hookCapturedValue);
		assertTrue("unexpected hookCapturedError: " + hookCapturedError, hookCapturedError instanceof IllegalArgumentException);
		assertEquals("boom", hookCapturedError.getMessage());
	}

	@Test
	public void errorCallbackError() {
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, v -> { throw new IllegalArgumentException("boom"); },
				ERROR);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over1", "over2")
		            .expectErrorMessage("boom")
		            .verify();

		assertNull("unexpected droppedValue", droppedValue);
		assertEquals("over3", hookCapturedValue);
		assertTrue("unexpected hookCapturedError: " + hookCapturedError, hookCapturedError instanceof IllegalArgumentException);
		assertEquals("boom", hookCapturedError.getMessage());
	}

	@Test
	public void noCallbackWithErrorStrategyOverflowsAfterDrain() {
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, null, ERROR);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onNext("over4");
			            processor.onComplete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(2)
		            .expectNext("over1", "over2")
		            .expectErrorMatches(Exceptions::isOverflow)
		            .verify();

		assertNull("unexpected droppedValue", droppedValue);
		assertEquals("over3", hookCapturedValue);
		assertTrue("unexpected hookCapturedError: " + hookCapturedError, hookCapturedError instanceof IllegalStateException);
		assertEquals("The receiver is overrun by more signals than expected (bounded queue...)", hookCapturedError.getMessage());
	}

	@Test
	public void noCallbackWithDropStrategyNoError() {
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, null, DROP_LATEST);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over1", "over2")
		            .expectComplete()
		            .verify();

		assertNull("unexpected droppedValue", droppedValue);
		assertNull("unexpected hookCapturedValue", hookCapturedValue);
		assertNull("unexpected hookCapturedError",hookCapturedError);
	}

	@Test
	public void noCallbackWithDropOldestStrategyNoError() {
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, null, DROP_OLDEST);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over2", "over3")
		            .expectComplete()
		            .verify();

		assertNull("unexpected droppedValue", droppedValue);
		assertNull("unexpected hookCapturedValue",hookCapturedValue);
		assertNull("unexpected hookCapturedError",hookCapturedError);
	}

	@Test
	public void fluxOnBackpressureBufferStrategyNoCallback() {
		DirectProcessor<String> processor = DirectProcessor.create();

		StepVerifier.create(processor.onBackpressureBuffer(2, DROP_OLDEST), 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over2", "over3")
		            .expectComplete()
		            .verify();

		assertNull("unexpected droppedValue", droppedValue);
		assertNull("unexpected hookCapturedValue",hookCapturedValue);
		assertNull("unexpected hookCapturedError",hookCapturedError);
	}

	@Test
	public void fluxOnBackpressureBufferStrategyRequiresCallback() {
		try {
			Flux.just("foo").onBackpressureBuffer(1,
					null,
					ERROR);
			fail("expected NullPointerException");
		}
		catch (NullPointerException e) {
			assertEquals("onBufferOverflow", e.getMessage());
		}
	}

	@Test
	public void fluxOnBackpressureBufferStrategyRequiresStrategy() {
		try {
			Flux.just("foo").onBackpressureBuffer(1,
					v -> { },
					null);
			fail("expected NullPointerException");
		}
		catch (NullPointerException e) {
			assertEquals("bufferOverflowStrategy", e.getMessage());
		}
	}

	@Test
    public void scanSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxOnBackpressureBufferStrategy.BackpressureBufferDropOldestSubscriber<Integer> test =
        		new FluxOnBackpressureBufferStrategy.BackpressureBufferDropOldestSubscriber<>(actual,
        				123, true, t -> {}, BufferOverflowStrategy.DROP_OLDEST);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
        test.requested = 35;
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
        test.offer(9);
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

        assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }
}