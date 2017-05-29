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
import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

public class FluxSkipUntilOtherTest extends FluxOperatorTest<String, String> {


	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.skipUntilOther(Flux.error(exception()))),
				scenario(f -> f.skipUntilOther(Flux.from(s -> {
					Operators.error(s, exception());

					//touch dropped items
					s.onNext(item(0));
					s.onNext(item(0));
					s.onComplete();
					s.onComplete();
				})))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.skipUntilOther(Flux.empty())),

				scenario(f -> Flux.<String>empty().skipUntilOther(f))
					.receiverEmpty()
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(
				scenario(f -> f.skipUntilOther(Flux.empty())).shouldHitDropNextHookAfterTerminate(false),

				scenario(f -> Flux.<String>never().skipUntilOther(f)).shouldHitDropNextHookAfterTerminate(false)
		);
	}

	@Test(expected = NullPointerException.class)
	public void nullSource() {
		new FluxSkipUntilOther<>(null, Flux.never());
	}

	@Test(expected = NullPointerException.class)
	public void nullOther() {
		Flux.never()
		    .skipUntilOther(null);
	}

	@Test
	public void skipNone() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .skipUntilOther(Flux.empty())
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void skipNoneBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .skipUntilOther(Flux.empty())
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
	public void skipAll() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .skipUntilOther(Flux.never())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void skipAllBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .skipUntilOther(Flux.never())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void skipNoneOtherMany() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .skipUntilOther(Flux.range(1, 10))
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void skipNoneBackpressuredOtherMany() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .skipUntilOther(Flux.range(1, 10))
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
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void otherSignalsError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .skipUntilOther(Flux.error(new RuntimeException("forced " + "failure")))
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
		    .skipUntilOther(Flux.error(new RuntimeException("forced " + "failure")))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	Flux<Integer> scenario_aFluxCanBeSkippedByTime(){
		return Flux.range(0, 1000)
		           .skip(Duration.ofSeconds(2));
	}

	@Test
	public void aFluxCanBeSkippedByTime(){
		StepVerifier.withVirtualTime(this::scenario_aFluxCanBeSkippedByTime)
		            .thenAwait(Duration.ofSeconds(2))
		            .verifyComplete();
	}


	Flux<Integer> scenario_aFluxCanBeSkippedByTime2(){
		return Flux.range(0, 1000)
		           .skip(Duration.ofMillis(2000));
	}

	@Test
	public void aFluxCanBeSkippedByTime2(){
		StepVerifier.withVirtualTime(this::scenario_aFluxCanBeSkippedByTime2)
		            .thenAwait(Duration.ofSeconds(2))
		            .verifyComplete();
	}

	Flux<Integer> scenario_aFluxCanBeSkippedByTimeZero(){
		return Flux.range(0, 1000)
		           .skip(Duration.ofMillis(0));
	}

	@Test
	public void aFluxCanBeSkippedByTimeZero(){
		StepVerifier.withVirtualTime(this::scenario_aFluxCanBeSkippedByTimeZero)
		            .thenAwait(Duration.ofSeconds(2))
		            .expectNextCount(1000)
		            .verifyComplete();
	}

	@Test
    public void scanMainSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSkipUntilOther.SkipUntilMainSubscriber<Integer> test =
        		new FluxSkipUntilOther.SkipUntilMainSubscriber<>(actual);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        SerializedSubscriber<?> serialized = (SerializedSubscriber<?>) test.scan(Scannable.ScannableAttr.ACTUAL);
        Assertions.assertThat(serialized.actual()).isSameAs(actual);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }

	@Test
    public void scanOtherSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSkipUntilOther.SkipUntilMainSubscriber<Integer> main =
        		new FluxSkipUntilOther.SkipUntilMainSubscriber<>(actual);
        FluxSkipUntilOther.SkipUntilOtherSubscriber<Integer> test =
        		new FluxSkipUntilOther.SkipUntilOtherSubscriber<>(main);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(main);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        main.cancel();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }
}
