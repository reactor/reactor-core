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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxBufferBoundaryTest
		extends FluxOperatorTest<String, List<String>> {

	@Override
	protected Scenario<String, List<String>> defaultScenarioOptions(Scenario<String, List<String>> defaultOptions) {
		return defaultOptions.prefetch(Integer.MAX_VALUE);
	}

	@Override
	protected List<Scenario<String, List<String>>> scenarios_operatorError() {
		return Arrays.asList(

				scenario(f -> f.buffer(Flux.never(), () -> null)),

				scenario(f -> f.buffer(Flux.never(), () -> {
					throw exception();
				})));
	}

	@Override
	protected List<Scenario<String, List<String>>> scenarios_operatorSuccess() {
		return Arrays.asList(scenario(f -> f.buffer(Mono.never()))
						.receive(i -> assertThat(i).containsExactly(item(0), item(1), item(2)))
				.shouldAssertPostTerminateState(false),

				scenario(f -> f.buffer(Mono.just(1)))
						.receiverEmpty()
						.shouldAssertPostTerminateState(false)
		);
	}

	@Override
	protected List<Scenario<String, List<String>>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(scenario(f -> f.buffer(Flux.never())));
	}

	@Test
	public void normal() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();

		FluxProcessorSink<Integer> sp1 = Processors.direct();
		FluxProcessorSink<Integer> sp2 = Processors.direct();

		sp1.asFlux().buffer(sp2.asFlux())
		   .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp1.next(1);
		sp1.next(2);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp2.next(1);

		ts.assertValues(Arrays.asList(1, 2))
		  .assertNoError()
		  .assertNotComplete();

		sp2.next(2);

		ts.assertValues(Arrays.asList(1, 2))
		  .assertNoError()
		  .assertNotComplete();

		sp1.next(3);
		sp1.next(4);

		sp2.complete();

		ts.assertValues(Arrays.asList(1, 2), Arrays.asList(3, 4))
		  .assertNoError()
		  .assertComplete();

		sp1.next(5);
		sp1.next(6);
		sp1.complete();

		ts.assertValues(Arrays.asList(1, 2), Arrays.asList(3, 4))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void mainError() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();

		FluxProcessorSink<Integer> sp1 = Processors.direct();
		FluxProcessorSink<Integer> sp2 = Processors.direct();

		sp1.asFlux().buffer(sp2.asFlux())
		   .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp1.next(1);
		sp1.next(2);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp2.next(1);

		ts.assertValues(Arrays.asList(1, 2))
		  .assertNoError()
		  .assertNotComplete();

		sp1.error(new RuntimeException("forced failure"));

		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());

		sp2.next(2);

		ts.assertValues(Arrays.asList(1, 2))
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		sp2.complete();

		ts.assertValues(Arrays.asList(1, 2))
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void otherError() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();

		FluxProcessorSink<Integer> sp1 = Processors.direct();
		FluxProcessorSink<Integer> sp2 = Processors.direct();

		sp1.asFlux().buffer(sp2.asFlux())
		   .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp1.next(1);
		sp1.next(2);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp2.next(1);

		ts.assertValues(Arrays.asList(1, 2))
		  .assertNoError()
		  .assertNotComplete();

		sp1.next(3);

		sp2.error(new RuntimeException("forced failure"));

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());

		ts.assertValues(Arrays.asList(1, 2))
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		sp2.complete();

		ts.assertValues(Arrays.asList(1, 2))
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void bufferSupplierThrows() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();

		FluxProcessorSink<Integer> sp1 = Processors.direct();
		FluxProcessorSink<Integer> sp2 = Processors.direct();

		sp1.asFlux().buffer(sp2.asFlux(), (Supplier<List<Integer>>) () -> {
			throw new RuntimeException("forced failure");
		})
		   .subscribe(ts);

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void bufferSupplierThrowsLater() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();

		FluxProcessorSink<Integer> sp1 = Processors.direct();
		FluxProcessorSink<Integer> sp2 = Processors.direct();

		int count[] = {1};

		sp1.asFlux().buffer(sp2.asFlux(), (Supplier<List<Integer>>) () -> {
			if (count[0]-- > 0) {
				return new ArrayList<>();
			}
			throw new RuntimeException("forced failure");
		})
		   .subscribe(ts);

		sp1.next(1);
		sp1.next(2);

		sp2.next(1);

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void bufferSupplierReturnsNUll() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();

		FluxProcessorSink<Integer> sp1 = Processors.direct();
		FluxProcessorSink<Integer> sp2 = Processors.direct();

		sp1.asFlux().buffer(sp2.asFlux(), (Supplier<List<Integer>>) () -> null)
		   .subscribe(ts);

		Assert.assertFalse("sp1 has subscribers?", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers?", sp2.hasDownstreams());

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	Flux<List<Integer>> scenario_bufferWillSubdivideAnInputFluxTime() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
		           .delayElements(Duration.ofMillis(99))
		           .buffer(Duration.ofMillis(200));
	}

	@Test
	public void bufferWillSubdivideAnInputFluxTime() {
		StepVerifier.withVirtualTime(this::scenario_bufferWillSubdivideAnInputFluxTime)
		            .thenAwait(Duration.ofSeconds(10))
		            .assertNext(t -> assertThat(t).containsExactly(1, 2))
		            .assertNext(t -> assertThat(t).containsExactly(3, 4))
		            .assertNext(t -> assertThat(t).containsExactly(5, 6))
		            .assertNext(t -> assertThat(t).containsExactly(7, 8))
		            .verifyComplete();
	}

	Flux<List<Integer>> scenario_bufferWillSubdivideAnInputFluxTime2() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
		           .delayElements(Duration.ofMillis(99))
		           .buffer(Duration.ofMillis(200));
	}

	@Test
	public void bufferWillSubdivideAnInputFluxTime2() {
		StepVerifier.withVirtualTime(this::scenario_bufferWillSubdivideAnInputFluxTime2)
		            .thenAwait(Duration.ofSeconds(10))
		            .assertNext(t -> assertThat(t).containsExactly(1, 2))
		            .assertNext(t -> assertThat(t).containsExactly(3, 4))
		            .assertNext(t -> assertThat(t).containsExactly(5, 6))
		            .assertNext(t -> assertThat(t).containsExactly(7, 8))
		            .verifyComplete();
	}

	@Test
	public void bufferWillAcumulateMultipleListsOfValues() {
		//given: "a source and a collected flux"
		FluxProcessorSink<Integer> numbers = Processors.emitter();

		//non overlapping buffers
		FluxProcessorSink<Integer> boundaryFlux = Processors.emitter();

		MonoProcessor<List<List<Integer>>> res = numbers.asFlux()
		                                                .buffer(boundaryFlux.asFlux())
		                                                .buffer()
		                                                .publishNext()
		                                                .toProcessor();
		res.subscribe();

		numbers.next(1);
		numbers.next(2);
		numbers.next(3);
		boundaryFlux.next(1);
		numbers.next(5);
		numbers.next(6);
		numbers.complete();

		//"the collected lists are available"
		assertThat(res.block()).containsExactly(Arrays.asList(1, 2, 3), Arrays.asList(5, 6));
	}

	@Test
	public void fluxEmptyBufferJust() {
//	    "flux empty buffer just"() {
//		when:
		List<List<Object>> ranges = Flux.empty()
		                                .buffer(Flux.just(1))
		                                .collectList()
		                                .block();

//		then:
		assertThat(ranges).isEmpty();
	}

	@Test
	public void fluxEmptyBuffer() {
//		"flux empty buffer"
//		when:
		List<List<Object>> ranges = Flux.empty()
		                                .buffer(Flux.never())
		                                .collectList()
		                                .block(Duration.ofMillis(100));

//		then:
		assertThat(ranges).isEmpty();
	}


	@Test
	public void scanMain() {
		CoreSubscriber<? super List> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		List<String> initialBuffer = Arrays.asList("foo", "bar");
		FluxBufferBoundary.BufferBoundaryMain<String, Integer, List<String>> test = new FluxBufferBoundary.BufferBoundaryMain<>(
				actual, initialBuffer, ArrayList::new);
		Subscription parent = Operators.cancelledSubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(2);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L);
		test.request(2);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(2L);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanOther() {
		FluxBufferBoundary.BufferBoundaryMain<String, Integer, List<String>> main = new FluxBufferBoundary.BufferBoundaryMain<>(
				null, null, ArrayList::new);
		FluxBufferBoundary.BufferBoundaryOther<Integer> test = new FluxBufferBoundary.BufferBoundaryOther<>(main);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		//the request is not tracked when there is a parent
		test.request(2);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanOtherRequestWhenNoParent() {
		FluxBufferBoundary.BufferBoundaryMain<String, Integer, List<String>> main = new FluxBufferBoundary.BufferBoundaryMain<>(
				null, null, ArrayList::new);
		FluxBufferBoundary.BufferBoundaryOther<Integer> test = new FluxBufferBoundary.BufferBoundaryOther<>(main);

		//the request is tracked when there is no parent
		test.request(2);
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(2L);
	}
}
