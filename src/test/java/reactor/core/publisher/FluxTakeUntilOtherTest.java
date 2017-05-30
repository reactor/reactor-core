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

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

public class FluxTakeUntilOtherTest {

	@Test(expected = NullPointerException.class)
	public void nullSource() {
		new FluxTakeUntilOther<>(null, Flux.never());
	}

	@Test(expected = NullPointerException.class)
	public void nullOther() {
		Flux.never()
		    .takeUntilOther(null);
	}

	@Test
	public void takeAll() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .takeUntilOther(Flux.never())
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void takeAllBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .takeUntilOther(Flux.never())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void takeNone() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .takeUntilOther(Flux.empty())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void takeNoneBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .takeUntilOther(Flux.empty())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void takeNoneOtherMany() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .takeUntilOther(Flux.range(1, 10))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void takeNoneBackpressuredOtherMany() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .takeUntilOther(Flux.range(1, 10))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void otherSignalsError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .takeUntilOther(Flux.error(new RuntimeException("forced " + "failure")))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void otherSignalsErrorBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .takeUntilOther(Flux.error(new RuntimeException("forced " + "failure")))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	Flux<Integer> scenario_aFluxCanBeLimitedByTime(){
		return Flux.range(0, 1000)
		           .take(Duration.ofSeconds(2));
	}

	@Test
	public void aFluxCanBeLimitedByTime(){
		StepVerifier.withVirtualTime(this::scenario_aFluxCanBeLimitedByTime)
		            .thenAwait(Duration.ofSeconds(2))
		            .expectNextCount(1000)
		            .verifyComplete();
	}

	Flux<Integer> scenario_aFluxCanBeLimitedByTime2(){
		return Flux.range(0, 1000)
		           .take(Duration.ofMillis(2000));
	}

	@Test
	public void aFluxCanBeLimitedByTime2(){
		StepVerifier.withVirtualTime(this::scenario_aFluxCanBeLimitedByTime2)
		            .thenAwait(Duration.ofSeconds(2))
		            .expectNextCount(1000)
		            .verifyComplete();
	}
	@Test
	public void aFluxCanBeLimitedByTime3(){
		StepVerifier.create(Flux.range(0, 1000).take(Duration.ofMillis(0L)))
		            .thenAwait(Duration.ofSeconds(2))
		            .verifyComplete();
	}

	@Test
    public void scanMainSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxTakeUntilOther.TakeUntilMainSubscriber<Integer> test =
        		new FluxTakeUntilOther.TakeUntilMainSubscriber<>(actual);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        @SuppressWarnings("unchecked")
		SerializedSubscriber<Integer> serialized = (SerializedSubscriber<Integer>) test.scan(Scannable.ScannableAttr.ACTUAL);
        Assertions.assertThat(serialized.actual()).isSameAs(actual);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }

	@Test
    public void scanOtherSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxTakeUntilOther.TakeUntilMainSubscriber<Integer> main =
        		new FluxTakeUntilOther.TakeUntilMainSubscriber<>(actual);
        FluxTakeUntilOther.TakeUntilOtherSubscriber<Integer> test =
        		new FluxTakeUntilOther.TakeUntilOtherSubscriber<>(main);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(main);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        main.cancel();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }
}
