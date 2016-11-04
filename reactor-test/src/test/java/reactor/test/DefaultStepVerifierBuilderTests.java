/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.Assert;
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * @author Stephane Maldini
 */
public class DefaultStepVerifierBuilderTests {


	@Test(expected = AssertionError.class)
	public void subscribedTwice() {
		Flux<String> flux = Flux.just("foo", "bar");

		DefaultVerifySubscriber<String> s =
				new DefaultStepVerifierBuilder<String>(Long.MAX_VALUE,
						null,
						null).expectNext("foo", "bar")
				             .expectComplete()
				.toSubscriber();

		flux.subscribe(s);
		flux.subscribe(s);
		s.verify();
	}

	@Test(timeout = 4000)
	public void manuallyManagedVirtualTime() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		try {
			VirtualTimeScheduler.enable(vts);

			Flux<String> flux = Flux.just("foo").delay(Duration.ofSeconds(4));

			DefaultVerifySubscriber<String> s =
					new DefaultStepVerifierBuilder<String>(Long.MAX_VALUE,
					null, //important to avoid triggering of vts capture-and-enable
					() -> vts)
					.thenAwait(Duration.ofSeconds(1))
					.expectNext("foo")
					.expectComplete()
					.toSubscriber();

			flux.subscribe(s);
			vts.advanceTimeBy(Duration.ofSeconds(3));
			s.verify();

			Assert.assertSame(vts, s.virtualTimeScheduler());
			Assert.assertSame(vts, VirtualTimeScheduler.get());
		}
		finally {
			VirtualTimeScheduler.reset();
		}
	}

	@Test
	public void suppliedVirtualTimeButNoSourceDoesntEnableScheduler() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();

		new DefaultStepVerifierBuilder<String>(Long.MAX_VALUE,
							null, //important to avoid triggering of vts capture-and-enable
							() -> vts)
							.expectNoEvent(Duration.ofSeconds(4))
							.expectComplete()
							.toSubscriber();

		try {
			//also test the side effect case where VTS has been enabled and not reset
			VirtualTimeScheduler current = VirtualTimeScheduler.get();
			Assert.assertNotSame(vts, current);
		}
		catch (IllegalStateException e) {
			assertThat(e.getMessage(), containsString("VirtualTimeScheduler"));
		}
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

		assertThat(queue.size(), is(5));
		assertThat(queue.poll(), is(instanceOf(TaskEvent.class)));
		assertThat(queue.poll(), is(instanceOf(TaskEvent.class)));
		assertThat(queue.poll(), is(instanceOf(WaitEvent.class)));
		assertThat(queue.poll(), is(instanceOf(DefaultStepVerifierBuilder.SubscriptionTaskEvent.class)));
		assertThat(queue.poll(), is(instanceOf(DefaultStepVerifierBuilder.SubscriptionTaskEvent.class)));
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

		assertThat(queue.size(), is(5));
		assertThat(queue.poll(), is(instanceOf(TaskEvent.class)));
		assertThat(queue.poll(), is(instanceOf(WaitEvent.class)));
		assertThat(queue.poll(), is(instanceOf(SignalCountEvent.class)));
		assertThat(queue.poll(), is(instanceOf(SubscriptionEvent.class)));
		assertThat(queue.poll(), is(instanceOf(SubscriptionEvent.class)));
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

		assertThat(queue.size(), is(3));

		SignalEvent<String> firstSignal = (SignalEvent<String>) queue.poll();
		assertThat(firstSignal.getDescription(), is("A"));

		SignalEvent<String> secondSignal = (SignalEvent<String>) queue.poll();
		assertThat(secondSignal.getDescription(), is("foo"));

		SignalCountEvent<String> thirdSignal = (SignalCountEvent<String>) queue.poll();
		assertThat(thirdSignal.getDescription(), is("baz"));
	}
}
