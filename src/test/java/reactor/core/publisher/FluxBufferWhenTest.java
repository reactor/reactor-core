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

import org.junit.Assert;
import org.junit.Test;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxBufferWhenTest {

	@Test
	public void normal() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();
		DirectProcessor<Integer> sp3 = DirectProcessor.create();
		DirectProcessor<Integer> sp4 = DirectProcessor.create();

		sp1.bufferWhen(sp2, v -> v == 1 ? sp3 : sp4)
		   .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp1.onNext(1);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp2.onNext(1);

		Assert.assertTrue("sp3 has no subscribers?", sp3.hasDownstreams());

		sp1.onNext(2);
		sp1.onNext(3);
		sp1.onNext(4);

		sp3.onComplete();

		ts.assertValues(Arrays.asList(2, 3, 4))
		  .assertNoError()
		  .assertNotComplete();

		sp1.onNext(5);

		sp2.onNext(2);

		Assert.assertTrue("sp4 has no subscribers?", sp4.hasDownstreams());

		sp1.onNext(6);

		sp4.onComplete();

		ts.assertValues(Arrays.asList(2, 3, 4), Arrays.asList(6))
		  .assertNoError()
		  .assertNotComplete();

		sp1.onComplete();

		ts.assertValues(Arrays.asList(2, 3, 4), Arrays.asList(6))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void startCompletes() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();
		DirectProcessor<Integer> sp3 = DirectProcessor.create();

		sp1.bufferWhen(sp2, v -> sp3)
		   .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp1.onNext(1);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp2.onNext(1);
		sp2.onComplete();

		Assert.assertTrue("sp3 has no subscribers?", sp3.hasDownstreams());

		sp1.onNext(2);
		sp1.onNext(3);
		sp1.onNext(4);

		sp3.onComplete();

		ts.assertValues(Arrays.asList(2, 3, 4))
		  .assertNoError()
		  .assertComplete();

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());
		Assert.assertFalse("sp3 has subscribers?", sp3.hasDownstreams());

	}


	@Test
	public void bufferWillAcumulateMultipleListsOfValuesOverlap() {
		//given: "a source and a collected flux"
		EmitterProcessor<Integer> numbers = EmitterProcessor.create();
		EmitterProcessor<Integer> bucketOpening = EmitterProcessor.create();

		//"overlapping buffers"
		EmitterProcessor<Integer> boundaryFlux = EmitterProcessor.create();

		Mono<List<List<Integer>>> res = numbers.bufferWhen(bucketOpening, u -> boundaryFlux )
		                                       .buffer()
		                                       .publishNext()
		                                       .subscribe();

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
		assertThat(res.block()).containsExactly(Arrays.asList(3, 5),
				Arrays.asList(5),
				Arrays.asList());
	}

	Flux<List<Integer>> scenario_bufferWillSubdivideAnInputFluxOverlapTime() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
		           .delayElements(Duration.ofMillis(99))
		           .buffer(Duration.ofMillis(300), Duration.ofMillis(200));
	}

	@Test
	public void bufferWillSubdivideAnInputFluxOverlapTime() {
		StepVerifier.withVirtualTime(this::scenario_bufferWillSubdivideAnInputFluxOverlapTime)
		            .thenAwait(Duration.ofSeconds(10))
		            .assertNext(t -> assertThat(t).containsExactly(1, 2, 3))
		            .assertNext(t -> assertThat(t).containsExactly(3, 4, 5))
		            .assertNext(t -> assertThat(t).containsExactly(5, 6, 7))
		            .assertNext(t -> assertThat(t).containsExactly(7, 8))
		            .verifyComplete();
	}

	Flux<List<Integer>> scenario_bufferWillSubdivideAnInputFluxOverlapTime2() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
		           .delayElements(Duration.ofMillis(99))
		           .buffer(Duration.ofMillis(300L), Duration.ofMillis(200L));
	}

	@Test
	public void bufferWillSubdivideAnInputFluxOverlapTime2() {
		StepVerifier.withVirtualTime(this::scenario_bufferWillSubdivideAnInputFluxOverlapTime2)
		            .thenAwait(Duration.ofSeconds(10))
		            .assertNext(t -> assertThat(t).containsExactly(1, 2, 3))
		            .assertNext(t -> assertThat(t).containsExactly(3, 4, 5))
		            .assertNext(t -> assertThat(t).containsExactly(5, 6, 7))
		            .assertNext(t -> assertThat(t).containsExactly(7, 8))
		            .verifyComplete();
	}

	Flux<List<Integer>> scenario_bufferWillSubdivideAnInputFluxSameTime() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
		           .delayElements(Duration.ofMillis(99))
		           .buffer(Duration.ofMillis(300L), Duration.ofMillis(300L));
	}

	@Test
	public void bufferWillSubdivideAnInputFluxSameTime() {
		StepVerifier.withVirtualTime(this::scenario_bufferWillSubdivideAnInputFluxSameTime)
		            .thenAwait(Duration.ofSeconds(10))
		            .assertNext(t -> assertThat(t).containsExactly(1, 2, 3))
		            .assertNext(t -> assertThat(t).containsExactly(4, 5, 6))
		            .assertNext(t -> assertThat(t).containsExactly(7, 8))
		            .verifyComplete();
	}

	Flux<List<Integer>> scenario_bufferWillSubdivideAnInputFluxGapTime() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
		           .delayElements(Duration.ofMillis(99))
		           .buffer(Duration.ofMillis(200), Duration.ofMillis(300));
	}

	@Test
	public void bufferWillSubdivideAnInputFluxGapTime() {
		StepVerifier.withVirtualTime(this::scenario_bufferWillSubdivideAnInputFluxGapTime)
		            .thenAwait(Duration.ofSeconds(10))
		            .assertNext(t -> assertThat(t).containsExactly(1, 2))
		            .assertNext(t -> assertThat(t).containsExactly(4, 5))
		            .assertNext(t -> assertThat(t).containsExactly(7, 8))
		            .verifyComplete();
	}
}
