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
import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.QueueSupplier;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxWindowWhenTest {

	static <T> AssertSubscriber<T> toList(Publisher<T> windows) {
		AssertSubscriber<T> ts = AssertSubscriber.create();
		windows.subscribe(ts);
		return ts;
	}

	@SafeVarargs
	static <T> void expect(AssertSubscriber<Flux<T>> ts, int index, T... values) {
		toList(ts.values()
		         .get(index)).assertValues(values)
		                     .assertComplete()
		                     .assertNoError();
	}

	@Test
	public void normal() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();
		DirectProcessor<Integer> sp3 = DirectProcessor.create();
		DirectProcessor<Integer> sp4 = DirectProcessor.create();

		sp1.windowWhen(sp2, v -> v == 1 ? sp3 : sp4)
		   .subscribe(ts);

		sp1.onNext(1);

		sp2.onNext(1);

		sp1.onNext(2);

		sp2.onNext(2);

		sp1.onNext(3);

		sp3.onNext(1);

		sp1.onNext(4);

		sp4.onNext(1);

		sp1.onComplete();

		ts.assertValueCount(2)
		  .assertNoError()
		  .assertComplete();

		expect(ts, 0, 2, 3);
		expect(ts, 1, 3, 4);

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());
		Assert.assertFalse("sp3 has subscribers?", sp3.hasDownstreams());
		Assert.assertFalse("sp4 has subscribers?", sp4.hasDownstreams());
	}

	@Test
	public void normalStarterEnds() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();
		DirectProcessor<Integer> sp3 = DirectProcessor.create();
		DirectProcessor<Integer> sp4 = DirectProcessor.create();

		sp1.windowWhen(sp2, v -> v == 1 ? sp3 : sp4)
		   .subscribe(ts);

		sp1.onNext(1);

		sp2.onNext(1);

		sp1.onNext(2);

		sp2.onNext(2);

		sp1.onNext(3);

		sp3.onNext(1);

		sp1.onNext(4);

		sp4.onNext(1);

		sp2.onComplete();

		ts.assertValueCount(2)
		  .assertNoError()
		  .assertComplete();

		expect(ts, 0, 2, 3);
		expect(ts, 1, 3, 4);

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());
		Assert.assertFalse("sp3 has subscribers?", sp3.hasDownstreams());
		Assert.assertFalse("sp4 has subscribers?", sp4.hasDownstreams());
	}

	@Test
	public void oneWindowOnly() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();
		DirectProcessor<Integer> sp3 = DirectProcessor.create();
		DirectProcessor<Integer> sp4 = DirectProcessor.create();

		sp1.windowWhen(sp2, v -> v == 1 ? sp3 : sp4)
		   .subscribe(ts);

		sp2.onNext(1);
		sp2.onComplete();

		sp1.onNext(1);
		sp1.onNext(2);
		sp1.onNext(3);

		sp3.onComplete();

		sp1.onNext(4);

		ts.assertValueCount(1)
		  .assertNoError()
		  .assertComplete();

		expect(ts, 0, 1, 2, 3);

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());
		Assert.assertFalse("sp3 has subscribers?", sp3.hasDownstreams());
		Assert.assertFalse("sp4 has subscribers?", sp4.hasDownstreams());
	}


	@Test
	public void windowWillAcumulateMultipleListsOfValuesOverlap() {
		//given: "a source and a collected flux"
		EmitterProcessor<Integer> numbers = EmitterProcessor.create();
		EmitterProcessor<Integer> bucketOpening = EmitterProcessor.create();

		//"overlapping buffers"
		EmitterProcessor<Integer> boundaryFlux = EmitterProcessor.create();

		MonoProcessor<List<List<Integer>>> res = numbers.windowWhen(bucketOpening, u -> boundaryFlux )
		                                       .flatMap(Flux::buffer)
		                                       .buffer()
		                                       .publishNext()
		                                       .toProcessor();
		res.subscribe();

		numbers.onNext(1);
		numbers.onNext(2);
		bucketOpening.onNext(1);
		numbers.onNext(3);
		bucketOpening.onNext(1);
		numbers.onNext(5);
		boundaryFlux.onNext(1);
		bucketOpening.onNext(1);
		boundaryFlux.onComplete();
		numbers.onComplete();

		//"the collected overlapping lists are available"
		assertThat(res.block()).containsExactly(
				Arrays.asList(3, 5),
				Arrays.asList(5));
	}



	Flux<List<Integer>> scenario_windowWillSubdivideAnInputFluxOverlapTime() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
		           .delayElements(Duration.ofMillis(99))
		           .window(Duration.ofMillis(300), Duration.ofMillis(200))
		           .concatMap(Flux::buffer);
	}

	@Test
	public void windowWillSubdivideAnInputFluxOverlapTime() {
		StepVerifier.withVirtualTime(this::scenario_windowWillSubdivideAnInputFluxOverlapTime)
		            .thenAwait(Duration.ofSeconds(10))
		            .assertNext(t -> assertThat(t).containsExactly(1, 2, 3))
		            .assertNext(t -> assertThat(t).containsExactly(3, 4, 5))
		            .assertNext(t -> assertThat(t).containsExactly(5, 6, 7))
		            .assertNext(t -> assertThat(t).containsExactly(7, 8))
		            .verifyComplete();
	}


	Flux<List<Integer>> scenario_windowWillSubdivideAnInputFluxSameTime() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
		           .delayElements(Duration.ofMillis(99))
		           .window(Duration.ofMillis(300), Duration.ofMillis(300))
		           .concatMap(Flux::buffer);
	}

	@Test
	public void windowWillSubdivideAnInputFluxSameTime() {
		StepVerifier.withVirtualTime(this::scenario_windowWillSubdivideAnInputFluxSameTime)
		            .thenAwait(Duration.ofSeconds(10))
		            .assertNext(t -> assertThat(t).containsExactly(1, 2, 3))
		            .assertNext(t -> assertThat(t).containsExactly(4, 5, 6))
		            .assertNext(t -> assertThat(t).containsExactly(7, 8))
		            .verifyComplete();
	}

	Flux<List<Integer>> scenario_windowWillSubdivideAnInputFluxGapTime() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
		           .delayElements(Duration.ofMillis(99))
		           .window(Duration.ofMillis(200), Duration.ofMillis(300))
		           .concatMap(Flux::buffer);
	}

	@Test
	public void windowWillSubdivideAnInputFluxGapTime() {
		StepVerifier.withVirtualTime(this::scenario_windowWillSubdivideAnInputFluxGapTime)
		            .thenAwait(Duration.ofSeconds(10))
		            .assertNext(t -> assertThat(t).containsExactly(1, 2))
		            .assertNext(t -> assertThat(t).containsExactly(4, 5))
		            .assertNext(t -> assertThat(t).containsExactly(7, 8))
		            .verifyComplete();
	}

	@Test
    public void scanMainSubscriber() {
        Subscriber<Flux<Integer>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxWindowWhen.WindowStartEndMainSubscriber<Integer, Integer, Integer> test =
        		new FluxWindowWhen.WindowStartEndMainSubscriber<>(actual, QueueSupplier.one().get(),
        		s -> Flux.just(s), QueueSupplier.unbounded());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

		Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.onComplete();
		Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
    }
}
