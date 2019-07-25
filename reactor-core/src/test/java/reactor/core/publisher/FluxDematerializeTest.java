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
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class FluxDematerializeTest extends FluxOperatorTest<Signal<String>, String> {

	@Override
	@SuppressWarnings("unchecked")
	protected List<Scenario<Signal<String>, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(Flux::<String>dematerialize)
					.producer(1, i -> Signal.complete())
					.receiverEmpty(),

				scenario(Flux::<String>dematerialize)
						.producer(1, i -> Signal.subscribe(Operators.emptySubscription()))
						.receiverEmpty()
		);
	}

	@Override
	protected List<Scenario<Signal<String>, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(Flux::<String>dematerialize)
						.producer(1, i -> Signal.error(exception())));
	}

	@Override
	protected List<Scenario<Signal<String>, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(
				scenario(Flux::<String>dematerialize)
		);
	}

	@Override
	protected Scenario<Signal<String>, String> defaultScenarioOptions(Scenario<Signal<String>, String> defaultOptions) {
		return defaultOptions.producer(3, i -> i == 0 ?
				Signal.next("test") :
				Signal.next("test"+i)
		).droppedItem(Signal.next("dropped"));
	}

	Signal<Integer> error = Signal.error(new RuntimeException("Forced failure"));

	@Test
	public void singleCompletion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> dematerialize = Flux.just(Signal.<Integer>complete()).dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void singleError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> dematerialize = Flux.just(error)
		                                  .dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertNotComplete();
	}

	@Test
	public void immediateCompletionNeedsRequestOne() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux<Integer> dematerialize = Flux.just(Signal.<Integer>complete()).dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);
		ts.assertComplete();
	}

	@Test
	public void immediateErrorNeedsRequestOne() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux<Integer> dematerialize = Flux.just(error).dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertNoError();

		ts.request(1);
		ts.assertError(RuntimeException.class);
	}

	@Test
	public void doesntCompleteWithoutRequest() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux<Integer> dematerialize = Flux.just(Signal.next(1), Signal.<Integer>complete()).dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);
		ts.assertComplete();
	}

	@Test
	public void doesntErrorWithoutRequest() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux<Integer> dematerialize = Flux.just(Signal.next(1), error).dematerialize();

		dematerialize.subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);
		ts.assertError(RuntimeException.class);
	}

	@Test
	public void twoSignalsAndComplete() {
		Flux<Integer> dematerialize = Flux.just(Signal.next(1), Signal.next(2), Signal.<Integer>complete())
		                                  .dematerialize();

		StepVerifier.create(dematerialize, 0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(50))
		            .thenRequest(1)
		            .expectNext(1)
		            .expectNoEvent(Duration.ofMillis(50))
		            .thenRequest(1)
		            .expectNext(2)
		            .expectNoEvent(Duration.ofMillis(50))
		            .thenRequest(1)
		            .verifyComplete();
	}

	@Test
	public void twoSignalsAndError() {
		Flux<Integer> dematerialize = Flux.just(Signal.next(1), Signal.next(2), error)
		                                  .dematerialize();

		StepVerifier.create(dematerialize, 0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(50))
		            .thenRequest(1)
		            .expectNext(1)
		            .expectNoEvent(Duration.ofMillis(50))
		            .thenRequest(1)
		            .expectNext(2)
		            .expectNoEvent(Duration.ofMillis(50))
		            .thenRequest(1)
		            .verifyError(error.getThrowable().getClass());
	}

	@Test
	public void neverEndingSignalSourceWithCompleteSignal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux<Integer> dematerialize = Flux.just(Signal.next(1), Signal.next(2),
				Signal.next(3), Signal.<Integer>complete())
		                                  .concatWith(Flux.never())
		                                  .dematerialize();

		dematerialize.subscribe(ts);

		ts.assertValues(1, 2, 3)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void dematerializeUnbounded() {
		StepVerifier.create(Flux.just(Signal.next("Three"),
				Signal.next("Two"),
				Signal.next("One"),
				Signal.complete())
		                        .dematerialize())
		            .expectNext("Three")
		            .expectNext("Two")
		            .expectNext("One")
		            .verifyComplete();
	}

	@Test
	public void materializeDematerializeUnbounded() {
		StepVerifier.create(Flux.just(1, 2, 3).materialize().dematerialize())
		            .expectNext(1, 2, 3)
		            .verifyComplete();
	}

	@Test
	public void materializeDematerializeRequestOneByOne() {
		StepVerifier.create(Flux.just(1, 2, 3).materialize().dematerialize(), 0)
		            .thenRequest(1)
		            .expectNext(1)
		            .thenRequest(1)
		            .expectNext(2)
		            .thenRequest(1)
		            .expectNext(3)
		            .expectNoEvent(Duration.ofMillis(50))
		            .thenRequest(1)
		            .verifyComplete();
	}

	@Test
	public void emissionTimingsAreGrouped() {
		StepVerifier.withVirtualTime(() ->
				Flux.interval(Duration.ofSeconds(1))
				    .map(i -> "tick" + i)
				    .take(5)
				    .timestamp()
				    .materialize()
				    .<Tuple2<Long, String>>dematerialize()
				    .timestamp()
		)
		            .thenAwait(Duration.ofSeconds(5))
		            .thenConsumeWhile(tupleDematerialize -> {
		            	long dematerializeTimestamp = tupleDematerialize.getT1();
		            	long originalTimestamp = tupleDematerialize.getT2().getT1();

		            	return dematerializeTimestamp == originalTimestamp;
		            })
		            .verifyComplete();
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null,
				sub -> sub.request(100));
		FluxDematerialize.DematerializeSubscriber<String> test =
				new FluxDematerialize.DematerializeSubscriber<>(actual, false);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.ERROR)).as("error is not retained").isNull();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}
}