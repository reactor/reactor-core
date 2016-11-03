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

import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

/**
 * @author Stephane Maldini
 */
public class DefaultStepVerifierBuilderTests {


	@Test(expected = AssertionError.class)
	public void subscribedTwice() {
		Flux<String> flux = Flux.just("foo", "bar");

		DefaultStepVerifierBuilder.DefaultVerifySubscriber<String> s =
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

			DefaultStepVerifierBuilder.DefaultVerifySubscriber<String> s =
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
}
