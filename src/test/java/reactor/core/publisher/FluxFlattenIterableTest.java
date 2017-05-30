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

import static org.assertj.core.api.Java6Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.Scannable.IntAttr;
import reactor.core.Scannable.LongAttr;
import reactor.core.Scannable.ScannableAttr;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.QueueSupplier;

public class FluxFlattenIterableTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.fusionMode(Fuseable.SYNC)
		                     .fusionModeThreadBarrier(Fuseable.ANY)
		                     .prefetch(QueueSupplier.SMALL_BUFFER_SIZE)
		                     .shouldHitDropNextHookAfterTerminate(false);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(

				scenario(f -> f.flatMapIterable(s -> Arrays.asList(s, (String)null)))
						.receiveValues(item(0)),

				scenario(f -> f.flatMapIterable(s -> null)),

				scenario(f -> f.flatMapIterable(s -> () -> null)),

				scenario(f -> f.flatMapIterable(s -> {
					throw exception();
				})),

				scenario(f -> f.flatMapIterable(s -> () -> new Iterator<String>() {
					@Override
					public boolean hasNext() {
						throw exception();
					}

					@Override
					public String next() {
						return null;
					}
				})),

				scenario(f -> f.flatMapIterable(s -> () -> new Iterator<String>() {
					@Override
					public boolean hasNext() {
						return true;
					}

					@Override
					public String next() {
						throw exception();
					}
				})),

				scenario(f -> f.flatMapIterable(s -> () -> new Iterator<String>() {
					boolean invoked;
					@Override
					public boolean hasNext() {
						if(!invoked){
							return true;
						}
						throw exception();
					}

					@Override
					public String next() {
						invoked = true;
						return item(0);
					}
				}))
						.fusionMode(Fuseable.NONE)
						.receiveValues(item(0))

		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.flatMapIterable(s -> Arrays.asList(s))),

				scenario(f -> f.flatMapIterable(s -> Arrays.asList(s), 1))
					.prefetch(1),

				scenario(f -> f.flatMapIterable(s -> new ArrayList<>()))
					.receiverEmpty(),

				scenario(f -> f.flatMapIterable(s -> Arrays.asList(s, s + s)))
						.receiveValues(
								item(0), item(0)+item(0),
								item(1), item(1)+item(1),
								item(2), item(2)+item(2))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(
				scenario(f -> f.flatMapIterable(s -> Arrays.asList(s, s + s)))
		);
	}

	@Test(expected=IllegalArgumentException.class)
	public void failPrefetch(){
		Flux.never()
	        .flatMapIterable(t -> null, -1);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5)
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertNoEvents();

		ts.request(1);

		ts.assertIncomplete(1);

		ts.request(2);

		ts.assertIncomplete(1, 2, 2);

		ts.request(7);

		ts.assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalNoFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5)
		    .hide()
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressuredNoFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5)
		    .hide()
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertNoEvents();

		ts.request(1);

		ts.assertIncomplete(1);

		ts.request(2);

		ts.assertIncomplete(1, 2, 2);

		ts.request(7);

		ts.assertValues(1, 2, 2, 3, 3, 4, 4, 5, 5, 6)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void longRunning() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		int n = 1_000_000;

		Flux.range(1, n)
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertValueCount(n * 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void longRunningNoFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		int n = 1_000_000;

		Flux.range(1, n)
		    .hide()
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertValueCount(n * 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void fullFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		int n = 1_000_000;

		Flux.range(1, n)
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .concatMap(Flux::just)
		    .subscribe(ts);

		ts.assertValueCount(n * 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void just() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1)
		    .concatMapIterable(v -> Arrays.asList(v, v + 1))
		    .subscribe(ts);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void empty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>empty().concatMapIterable(v -> Arrays.asList(v, v + 1))
		                     .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	/**
	 * See https://github.com/reactor/reactor-core/issues/453
	 */
	@Test
	public void testDrainSyncCompletesSeveralBatches() {
		//both hide and just with 2 elements are necessary to go into SYNC mode
		StepVerifier.create(Flux.just(1, 2)
		                        .flatMapIterable(t -> IntStream.rangeClosed(0, 35).boxed().collect(Collectors.toList()))
		                        .hide()
                                .zipWith(Flux.range(1000, 100))
		                        .count())
		            .expectNext(72L)
		            .verifyComplete();
	}

	/**
	 * See https://github.com/reactor/reactor-core/issues/453
	 */
	@Test
	public void testDrainAsyncCompletesSeveralBatches() {
		StepVerifier.create(Flux.range(0, 72)
		                        .collectList()
		                        .flatMapIterable(Function.identity())
		                        .zipWith(Flux.range(1000, 100))
		                        .count())
		            .expectNext(72L)
		            .verifyComplete();
	}

	/**
	 * See https://github.com/reactor/reactor-core/issues/508
	 */
	@Test
	public void testPublishingTwice() {
		StepVerifier.create(Flux.just(Flux.range(0, 300).toIterable(), Flux.range(0, 300).toIterable())
				.flatMapIterable(x -> x)
				.share()
				.share()
				.count())
				.expectNext(600L)
				.verifyComplete();
	}

    @Test
    public void scanOperator() {
        Flux<Integer> source = Flux.range(1, 10);
        FluxFlattenIterable<Integer, Integer> test = new FluxFlattenIterable<>(source, i -> new ArrayList<>(i), 35, QueueSupplier.one());

        assertThat(test.scan(ScannableAttr.PARENT)).isSameAs(source);
        assertThat(test.scan(IntAttr.PREFETCH)).isEqualTo(35);
    }

    @Test
    public void scanSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxFlattenIterable.FlattenIterableSubscriber<Integer, Integer> test =
                new FluxFlattenIterable.FlattenIterableSubscriber<>(actual, i -> new ArrayList<>(i), 123, QueueSupplier.<Integer>one());
        Subscription s = Operators.emptySubscription();
        test.onSubscribe(s);

        assertThat(test.scan(ScannableAttr.PARENT)).isSameAs(s);
        assertThat(test.scan(ScannableAttr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(IntAttr.PREFETCH)).isEqualTo(123);
        test.requested = 35;
        assertThat(test.scan(LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
        test.queue.add(5);
        assertThat(test.scan(IntAttr.BUFFERED)).isEqualTo(1);

        assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isNull();
        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.error = new IllegalStateException("boom");
        assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");
        test.onComplete();
        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();

    }
}
