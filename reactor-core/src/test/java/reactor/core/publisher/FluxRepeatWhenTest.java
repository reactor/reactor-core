/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

public class FluxRepeatWhenTest {

	@Test(expected = NullPointerException.class)
	public void whenFactoryNull() {
		Flux.never()
		    .repeatWhen(null);
	}

	@Test
	public void coldRepeater() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1)
		    .repeatWhen(v -> Flux.range(1, 10))
		    .subscribe(ts);

		ts.assertValues(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void coldRepeaterBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 2)
		    .repeatWhen(v -> Flux.range(1, 5))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2, 1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void coldEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 2)
		    .repeatWhen(v -> Flux.empty())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void coldError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 2)
		    .repeatWhen(v -> Flux.error(new RuntimeException("forced " + "failure")))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void whenFactoryThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .repeatWhen(v -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

	}

	@Test
	public void whenFactoryReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		new FluxRepeatWhen<>(Flux.range(1, 2), v -> null).subscribe(ts);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();

	}

	@Test
	public void repeaterErrorsInResponse() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .repeatWhen(v -> v.map(a -> {
			    throw new RuntimeException("forced failure");
		    }))
		    .subscribe(ts);

		ts.assertValues(1, 2)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

	}

	@Test
	public void retryAlways() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 2)
		    .repeatWhen(v -> v)
		    .subscribe(ts);

		ts.request(8);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2)
		  .assertNoError()
		  .assertNotComplete();
	}

	@Test
	public void retryAlwaysScalar() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		AtomicInteger count = new AtomicInteger();

		Flux.just(1)
		    .map(d -> count.incrementAndGet())
		    .repeatWhen(v -> v)
		    .subscribe(ts);

		ts.request(8);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8)
		  .assertNoError()
		  .assertNotComplete();
	}

	@Test
	public void retryWithVolumeCondition() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 2)
		    .repeatWhen(v -> v.takeWhile(n -> n > 0))
		    .subscribe(ts);

		ts.request(8);

		ts.assertValues(1, 2, 1, 2, 1, 2, 1, 2)
		  .assertNoError()
		  .assertNotComplete();
	}

	@Test
	public void exponentialRepeat() {
		StepVerifier.withVirtualTime(this::exponentialRepeatScenario1)
		            .expectNext(1)
		            .thenAwait(Duration.ofSeconds(1))
		            .expectNext(2)
		            .thenAwait(Duration.ofSeconds(2))
		            .expectNext(3)
		            .thenAwait(Duration.ofSeconds(3))
		            .expectNext(4)
		            .expectComplete()
		            .verify();
	}

	Flux<Integer> exponentialRepeatScenario1() {
		AtomicInteger i = new AtomicInteger();
		return Mono.fromCallable(i::incrementAndGet)
		           .repeatWhen(repeat -> repeat.zipWith(Flux.range(1, 3), (t1, t2) -> t2)
		                                       .flatMap(time -> Mono.delay(Duration.ofSeconds(
				                                       time))));
	}

	@Test
	public void exponentialRepeat2() {
		StepVerifier.withVirtualTime(this::exponentialRepeatScenario2)
		            .thenAwait(Duration.ofSeconds(6))
		            .expectNext("hey")
		            .expectComplete()
		            .verify();
	}

	Flux<String> exponentialRepeatScenario2() {
		AtomicInteger i = new AtomicInteger();
		return Mono.<String>create(s -> {
			if (i.incrementAndGet() == 4) {
				s.success("hey");
			}
			else {
				s.success();
			}
		}).repeatWhen(repeat -> repeat.zipWith(Flux.range(1, 3), (t1, t2) -> t2)
		                              .flatMap(time -> Mono.delay(Duration.ofSeconds(time))));
	}

	@Test
    public void scanMainSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxRepeatWhen.RepeatWhenMainSubscriber<Integer> test =
        		new FluxRepeatWhen.RepeatWhenMainSubscriber<>(actual, null, Flux.empty());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
        test.requested = 35;
        Assertions.assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35L);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }

	@Test
    public void scanOtherSubscriber() {
		Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxRepeatWhen.RepeatWhenMainSubscriber<Integer> main =
        		new FluxRepeatWhen.RepeatWhenMainSubscriber<>(actual, null, Flux.empty());
        FluxRepeatWhen.RepeatWhenOtherSubscriber test = new FluxRepeatWhen.RepeatWhenOtherSubscriber();
        test.main = main;

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(main.otherArbiter);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(main);
    }
}
