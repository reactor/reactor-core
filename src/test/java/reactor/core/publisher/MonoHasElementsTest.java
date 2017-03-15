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

package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoHasElementsTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new MonoHasElements<>(null);
	}

	@Test
	public void emptySource() {
		AssertSubscriber<Boolean> ts = AssertSubscriber.create();

		Mono.empty().hasElement().subscribe(ts);

		ts.assertValues(false)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void emptySourceBackpressured() {
		AssertSubscriber<Boolean> ts = AssertSubscriber.create(0);

		Mono.empty().hasElement().subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(1);

		ts.assertValues(false)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void nonEmptySource() {
		AssertSubscriber<Boolean> ts = AssertSubscriber.create();

		Flux.range(1, 10).hasElements().subscribe(ts);

		ts.assertValues(true)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void nonEmptySourceBackpressured() {
		AssertSubscriber<Boolean> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).hasElements().subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(1);

		ts.assertValues(true)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void fluxSourceIsCancelled() {
		AtomicLong cancelCount = new AtomicLong();

		StepVerifier.create(Flux.range(1, 10)
		                        .doOnCancel(cancelCount::incrementAndGet)
		                        .hasElements())
	                .expectNext(true)
	                .verifyComplete();

		assertThat(cancelCount.get()).isEqualTo(1);
	}

	@Test
	public void monoSourceIsNotCancelled() {
		AtomicLong cancelCount = new AtomicLong();

		StepVerifier.create(Mono.just(1)
		                        .doOnCancel(cancelCount::incrementAndGet)
		                        .hasElement())
		            .expectNext(true)
		            .verifyComplete();

		assertThat(cancelCount.get()).isEqualTo(0);
	}
}
