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
package reactor.test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.DefaultStepVerifierBuilder.DefaultVerifySubscriber;
import reactor.test.DefaultStepVerifierBuilder.DescriptionEvent;
import reactor.test.DefaultStepVerifierBuilder.Event;
import reactor.test.DefaultStepVerifierBuilder.SignalCountEvent;
import reactor.test.DefaultStepVerifierBuilder.SignalEvent;
import reactor.test.DefaultStepVerifierBuilder.SubscriptionEvent;
import reactor.test.DefaultStepVerifierBuilder.TaskEvent;
import reactor.test.DefaultStepVerifierBuilder.WaitEvent;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Stephane Maldini
 */
public class DefaultStepVerifierBuilderTests {

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

	@Test(timeout = 4000)
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
}
