/*
 * Copyright (c) 2017-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.test.DefaultStepVerifierBuilder.DefaultVerifySubscriber;
import reactor.test.DefaultStepVerifierBuilder.DescriptionEvent;
import reactor.test.DefaultStepVerifierBuilder.Event;
import reactor.test.DefaultStepVerifierBuilder.SignalCountEvent;
import reactor.test.DefaultStepVerifierBuilder.SignalEvent;
import reactor.test.DefaultStepVerifierBuilder.SubscriptionEvent;
import reactor.test.DefaultStepVerifierBuilder.TaskEvent;
import reactor.test.DefaultStepVerifierBuilder.WaitEvent;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.retry.Retry;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Stephane Maldini
 */
public class DefaultStepVerifierBuilderTests {

	@Test
	void consumeWhileFollowedByTaskEvent() {
		List<Integer> consumed = new ArrayList<>();

		Flux.range(1, 20)
			.as(StepVerifier::create)
			.thenConsumeWhile(i -> i < 18, consumed::add)
			.then(() -> consumed.add(100))
			.expectNext(18, 19, 20)
			.verifyComplete();

		assertThat(consumed)
			.containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
				11, 12, 13, 14, 15, 16, 17, 100);
	}

	@Test
	void thenConsumeWhile_NoElementsShouldBeConsumed_oneElement() {
		Flux<String> given = Flux.just("a");

		StepVerifier.create(given)
			.thenConsumeWhile(s -> s.equals("42"))
			.then(() -> {}) // do nothing
			.consumeNextWith(s -> Assertions.assertEquals("a", s))
			.verifyComplete();

		// expectation "consumeNextWith" failed (expected: onNext(); actual: onComplete())
	}

	@Test
	void thenConsumeWhile_NoElementsShouldBeConsumed_consumeWhileTwice() {
		Flux<String> given = Flux.just("a", "a");

		StepVerifier.create(given)
			.thenConsumeWhile(s -> s.equals("42"))
			.then(() -> {})
			.thenConsumeWhile(s -> s.equals("42"))
			.then(() -> {})
			.consumeNextWith(s -> Assertions.assertEquals("a", s))
			.consumeNextWith(s -> Assertions.assertEquals("a", s))
			.verifyComplete();

		// expectation "consumeNextWith" failed (expected: onNext(); actual: onComplete())
	}

	@Test
	void thenConsumeWhile_NoElementsShouldBeConsumed_moreThanOneElement() {
		Flux<String> given = Flux.just("a", "b");

		StepVerifier.create(given)
			.thenConsumeWhile(s -> s.equals("42"))
			.then(() -> {}) // do nothing
			.consumeNextWith(s -> Assertions.assertEquals("a", s))
			.consumeNextWith(s -> Assertions.assertEquals("b", s))
			.verifyComplete();

		// org.opentest4j.AssertionFailedError: expected: <a> but was: <b>
	}

	@Test
	void thenConsumeWhile_OneElementShouldBeConsumed() {
		Flux<String> given = Flux.just("a", "b");

		StepVerifier.create(given)
			.thenConsumeWhile(s -> s.equals("a"))
			.then(() -> {}) // do nothing
			.consumeNextWith(s -> Assertions.assertEquals("b", s))
			.verifyComplete();

		// expectation "consumeNextWith" failed (expected: onNext(); actual: onComplete())
	}

	@Test
	void thenConsumeWhile_onlyTwoElementsShouldBeConsumed() {
		Flux<String> given = Flux.just("a", "a", "b");

		StepVerifier.create(given)
			.thenConsumeWhile(s -> s.equals("a"))
			.then(() -> {}) // do nothing
			.consumeNextWith(s -> Assertions.assertEquals("b", s))
			.verifyComplete();

		// expectation "consumeNextWith" failed (expected: onNext(); actual: onComplete())
	}

	@Test
	public void subscribedTwice() {
		Flux<String> flux = Flux.just("foo", "bar");

		DefaultVerifySubscriber<String> s =
				new DefaultStepVerifierBuilder<String>(StepVerifierOptions.create().initialRequest(Long.MAX_VALUE), null)
						.expectNext("foo", "bar")
						.expectComplete()
				.toSubscriber();

		flux.subscribe(s);
		flux.subscribe(s);
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(s::verify)
				.withMessageStartingWith("expectation failed (an unexpected Subscription has been received");
	}

	@Test
	public void subscribedTwiceDetectsSpecialSubscription() {
		Flux<String> flux = Flux.never();

		DefaultVerifySubscriber<String> s =
				new DefaultStepVerifierBuilder<String>(StepVerifierOptions.create().initialRequest(Long.MAX_VALUE), null)
						.expectErrorMessage("expected")
				.toSubscriber();

		flux.subscribe(s);
		Operators.reportThrowInSubscribe(s, new RuntimeException("expected"));

		s.verify(Duration.ofSeconds(1));
	}

	@Test
	@Timeout(4)
	public void manuallyManagedVirtualTime() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		try {
			VirtualTimeScheduler.getOrSet(vts);
			assertThat(VirtualTimeScheduler.get()).isSameAs(vts);

			Flux<String> flux = Flux.just("foo").delayElements(Duration.ofSeconds(4));

			DefaultVerifySubscriber<String> s =
					new DefaultStepVerifierBuilder<String>(StepVerifierOptions.create()
							.initialRequest(Long.MAX_VALUE)
							.virtualTimeSchedulerSupplier(() -> vts),
					null)//important to avoid triggering of vts capture-and-enable
					.thenAwait(Duration.ofSeconds(1))
					.expectNext("foo")
					.expectComplete()
					.toSubscriber();

			flux.subscribe(s);
			vts.advanceTimeBy(Duration.ofSeconds(3));
			s.verify();

			assertThat(s.virtualTimeScheduler()).isSameAs(vts);
			assertThat(VirtualTimeScheduler.get()).isSameAs(vts);
		}
		finally {
			VirtualTimeScheduler.reset();
		}
	}

	@Test
	public void suppliedVirtualTimeButNoSourceDoesntEnableScheduler() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();

		new DefaultStepVerifierBuilder<String>(StepVerifierOptions.create()
				.initialRequest(Long.MAX_VALUE)
				.virtualTimeSchedulerSupplier(() -> vts),
				null) //important to avoid triggering of vts capture-and-enable
							.expectNoEvent(Duration.ofSeconds(4))
							.expectComplete()
							.toSubscriber();

		try {
			//also test the side effect case where VTS has been enabled and not reset
			VirtualTimeScheduler current = VirtualTimeScheduler.get();
			assertThat(current).isNotSameAs(vts);
		}
		catch (IllegalStateException e) {
			assertThat(e).hasMessageContaining("VirtualTimeScheduler");
		}
	}

	@Test
	public void noSourceSupplierFailsFastWhenAttemptingVerify() {
		StepVerifier stepVerifier = new DefaultStepVerifierBuilder<String>(StepVerifierOptions.create(), null).expectComplete();

		assertThatIllegalArgumentException().isThrownBy(stepVerifier::verify)
		                                    .withMessage("no source to automatically subscribe to for verification");
	}

	@Test
	public void testConflateOnTaskThenSubscriptionEvents() {
		List<Event<String>> script = Arrays.asList(
				new TaskEvent<String>(() -> {}, "A"),
				new TaskEvent<String>(() -> {}, "B"),
				new WaitEvent<String>(Duration.ofSeconds(5), "C"),
				new SubscriptionEvent<String>("D"),
				new SubscriptionEvent<String>(sub -> { }, "E")
		);

		Queue<Event<String>> queue =
				DefaultVerifySubscriber.conflateScript(script, null);

		assertThat(queue)
				.hasSize(5)
				.extracting(e -> e.getClass().getName())
				.containsExactly(
				        TaskEvent.class.getName(),
				        TaskEvent.class.getName(),
				        WaitEvent.class.getName(),
				        DefaultStepVerifierBuilder.SubscriptionTaskEvent.class.getName(),
				        DefaultStepVerifierBuilder.SubscriptionTaskEvent.class.getName());
	}

	@Test
	public void testNoConflateOnSignalThenSubscriptionEvents() {
		List<Event<String>> script = Arrays.asList(
				new TaskEvent<String>(() -> {}, "A"),
				new WaitEvent<String>(Duration.ofSeconds(5), "B"),
				new SignalCountEvent<>(3, "C"),
				new SubscriptionEvent<String>("D"),
				new SubscriptionEvent<String>(sub -> { }, "E")
		);

		Queue<Event<String>> queue =
				DefaultVerifySubscriber.conflateScript(script, null);

		assertThat(queue)
				.hasSize(5)
				.extracting(e -> e.getClass().getName())
				.containsExactly(
						TaskEvent.class.getName(),
						WaitEvent.class.getName(),
						SignalCountEvent.class.getName(),
						SubscriptionEvent.class.getName(),
						SubscriptionEvent.class.getName());
	}

	@Test
	public void testConflateChangesDescriptionAndRemoveAs() {
		List<Event<String>> script = Arrays.asList(
				new SignalEvent<String>((s,v) -> Optional.empty(), "A"),
				new SignalEvent<String>((s,v) -> Optional.empty(), "B"),
				new DescriptionEvent<String>("foo"),
				new DescriptionEvent<String>("bar"),
				new SignalCountEvent<String>(1, "C"),
				new DescriptionEvent<String>("baz")
		);

		Queue<Event<String>> queue = DefaultVerifySubscriber.conflateScript(script, null);

		assertThat(queue).hasSize(3)
		                 .extracting(Event::getDescription)
		                 .containsExactly("A", "foo", "baz");
	}

	@Test
	void enableConditionalSupportProducesAConditionalSubscriber() {
		DefaultStepVerifierBuilder<Object> builder = new DefaultStepVerifierBuilder<>(StepVerifierOptions.create(), Mono::empty);

		assertThat(builder.build().toSubscriber())
			.as("without enableConditionalSupport")
			.isNotInstanceOf(Fuseable.ConditionalSubscriber.class);

		assertThat(builder.enableConditionalSupport(i -> true).build().toSubscriber())
			.as("with enableConditionalSupport")
			.isInstanceOf(Fuseable.ConditionalSubscriber.class);
	}

	@Test
	void enableConditionalSupportProducesAConditionalSubscriberAndSubscribes() {
		DefaultStepVerifierBuilder<Object> builder = new DefaultStepVerifierBuilder<>(StepVerifierOptions.create(), Mono::empty);

		assertThat(builder.build().toVerifierAndSubscribe())
			.as("without enableConditionalSupport")
			.isNotInstanceOf(Fuseable.ConditionalSubscriber.class);

		assertThat(builder.enableConditionalSupport(i -> true).build().toVerifierAndSubscribe())
			.as("with enableConditionalSupport")
			.isInstanceOf(Fuseable.ConditionalSubscriber.class);
	}

	@Test
	@Timeout(5) // Ensure test completes within 5 seconds (should be much faster with the fix)
	void retryWhenBackoffDoesNotCauseResourceExhaustion() {
		// Reproduces issue #4054: retryWhen(backoff()) causing high resource usage
		// The fix prevents busy-waiting by using adaptive wait times when idle
		AtomicInteger callCount = new AtomicInteger(0);

		Flux<Object> testFlux = Flux.generate(
				sink -> {
					int count = callCount.incrementAndGet();
					if (count <= 2) {
						sink.error(new IllegalStateException("Error " + count));
					}
					else {
						sink.next(1);
						sink.complete();
					}
				})
				.retryWhen(Retry.backoff(3, Duration.ofMillis(10)));

		// This should complete quickly without consuming excessive resources
		// Before the fix, this would cause OOM due to busy-waiting
		StepVerifier.create(testFlux)
				.expectNext(1)
				.expectComplete()
				.verify(Duration.ofSeconds(2));

		assertThat(callCount.get()).isEqualTo(3);
	}
}
