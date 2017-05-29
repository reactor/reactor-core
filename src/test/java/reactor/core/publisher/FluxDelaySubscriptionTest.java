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

import java.time.Duration;

import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxDelaySubscriptionTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxDelaySubscription<>(null, Flux.never());
	}

	@Test(expected = NullPointerException.class)
	public void otherNull() {
		Flux.never().delaySubscription((Publisher<?>)null);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .delaySubscription(Flux.just(1))
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .delaySubscription(Flux.just(1))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);
		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void manyTriggered() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .delaySubscription(Flux.range(1, 10))
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void manyTriggeredBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .delaySubscription(Flux.range(1, 10))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);
		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void emptyTrigger() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .delaySubscription(Flux.empty())
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void emptyTriggerBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .delaySubscription(Flux.empty())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);
		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void neverTriggered() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .delaySubscription(Flux.never())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();
	}



	Flux<Integer> scenario_delayedTrigger(){
		return Flux.just(1)
		           .delaySubscription(Duration.ofSeconds(3));
	}

	@Test
	public void delayedTrigger() {
		StepVerifier.withVirtualTime(this::scenario_delayedTrigger)
		            .thenAwait(Duration.ofSeconds(3))
		            .expectNext(1)
		            .verifyComplete();
	}

	Flux<Integer> scenario_delayedTrigger2(){
		return Flux.just(1)
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
	public void scanMainSubscriber() {
		Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null,
				sub -> sub.request(100));
		Flux<String> source = Flux.just("foo", "bar");
		FluxDelaySubscription.DelaySubscriptionOtherSubscriber<String, Integer> arbiter = new FluxDelaySubscription.DelaySubscriptionOtherSubscriber<String, Integer>(
				actual, source);
		FluxDelaySubscription.DelaySubscriptionMainSubscriber<String> test = new FluxDelaySubscription.DelaySubscriptionMainSubscriber<String>(
				actual, arbiter);

		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
	}

	@Test
	public void scanOtherSubscriber() {
		Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null,
				sub -> sub.request(100));
		Flux<String> source = Flux.just("foo", "bar");
		FluxDelaySubscription.DelaySubscriptionOtherSubscriber<String, Integer> test = new FluxDelaySubscription.DelaySubscriptionOtherSubscriber<String, Integer>(
				actual, source);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(100L);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}

}
