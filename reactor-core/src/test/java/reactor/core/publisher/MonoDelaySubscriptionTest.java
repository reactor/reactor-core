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

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThat;

public class MonoDelaySubscriptionTest {

	@Test
	public void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoDelaySubscription<>(null, Mono.never());
		});
	}

	@Test
	public void otherNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Mono.never().delaySubscription((Publisher<?>) null);
		});
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1)
		    .delaySubscription(Mono.just(1))
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	Mono<Integer> scenario_delayedTrigger(){
		return Mono.just(1)
		           .delaySubscription(Duration.ofSeconds(3));
	}

	@Test
	public void delayedTrigger() {
		StepVerifier.withVirtualTime(this::scenario_delayedTrigger)
		            .thenAwait(Duration.ofSeconds(3))
		            .expectNext(1)
		            .verifyComplete();
	}

	Mono<Integer> scenario_delayedTrigger2(){
		return Mono.just(1)
		           .delaySubscription(Duration.ofMillis(50));
	}

	@Test
	public void delayedTrigger2() {
		StepVerifier.withVirtualTime(this::scenario_delayedTrigger2)
		            .thenAwait(Duration.ofMillis(50))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.just(1)
		    .delaySubscription(Mono.just(1))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void manyTriggered() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1)
		    .delaySubscription(Flux.range(1, 10))
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void manyTriggeredBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.just(1)
		    .delaySubscription(Flux.range(1, 10))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void emptyTrigger() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1)
		    .delaySubscription(Mono.empty())
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void emptyTriggerBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.just(1)
		    .delaySubscription(Mono.empty())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void neverTriggered() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1)
		    .delaySubscription(Mono.never())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();
	}

	@Test
	public void scanOperator(){
		MonoDelaySubscription<Integer, Integer> test = new MonoDelaySubscription<>(Mono.just(1), Mono.just(2));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
