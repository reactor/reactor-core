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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FluxTakeUntilOtherTest {

	@Test
	public void nullSource() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new FluxTakeUntilOther<>(null, Flux.never());
		});
	}

	@Test
	public void nullOther() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never()
					.takeUntilOther(null);
		});
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
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxTakeUntilOther<Integer, Integer> test = new FluxTakeUntilOther<>(parent, Flux.just(2));

		Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    public void scanMainSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxTakeUntilOther.TakeUntilMainSubscriber<Integer> test =
        		new FluxTakeUntilOther.TakeUntilMainSubscriber<>(actual);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        @SuppressWarnings("unchecked")
		SerializedSubscriber<Integer> serialized = (SerializedSubscriber<Integer>) test.scan(Scannable.Attr.ACTUAL);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        Assertions.assertThat(serialized).isNotNull();
        Assertions.assertThat(serialized.actual()).isSameAs(actual);

        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanOtherSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxTakeUntilOther.TakeUntilMainSubscriber<Integer> main =
        		new FluxTakeUntilOther.TakeUntilMainSubscriber<>(actual);
        FluxTakeUntilOther.TakeUntilOtherSubscriber<Integer> test =
        		new FluxTakeUntilOther.TakeUntilOtherSubscriber<>(main);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        main.cancel();
        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }
}
