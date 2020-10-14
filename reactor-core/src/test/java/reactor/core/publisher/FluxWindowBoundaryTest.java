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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxWindowBoundaryTest {

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

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .window(sp2.asFlux())
		   .subscribe(ts);

		ts.assertValueCount(1);

		sp1.emitNext(1, FAIL_FAST);
		sp1.emitNext(2, FAIL_FAST);
		sp1.emitNext(3, FAIL_FAST);

		sp2.emitNext(1, FAIL_FAST);

		sp1.emitNext(4, FAIL_FAST);
		sp1.emitNext(5, FAIL_FAST);

		sp1.emitComplete(FAIL_FAST);

		ts.assertValueCount(2);

		expect(ts, 0, 1, 2, 3);
		expect(ts, 1, 4, 5);

		ts.assertNoError()
		  .assertComplete();

		assertThat(sp1.currentSubscriberCount()).as("sp1 has subscriber").isZero();
		assertThat(sp2.currentSubscriberCount()).as("sp2 has subscriber").isZero();
	}

	@Test
	public void normalOtherCompletes() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .window(sp2.asFlux())
		   .subscribe(ts);

		ts.assertValueCount(1);

		sp1.emitNext(1, FAIL_FAST);
		sp1.emitNext(2, FAIL_FAST);
		sp1.emitNext(3, FAIL_FAST);

		sp2.emitNext(1, FAIL_FAST);

		sp1.emitNext(4, FAIL_FAST);
		sp1.emitNext(5, FAIL_FAST);

		sp2.emitComplete(FAIL_FAST);

		ts.assertValueCount(2);

		expect(ts, 0, 1, 2, 3);
		expect(ts, 1, 4, 5);

		ts.assertNoError()
		  .assertComplete();

		assertThat(sp1.currentSubscriberCount()).as("sp1 has subscriber").isZero();
		assertThat(sp2.currentSubscriberCount()).as("sp2 has subscriber").isZero();
	}

	@Test
	public void mainError() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .window(sp2.asFlux())
		   .subscribe(ts);

		ts.assertValueCount(1);

		sp1.emitNext(1, FAIL_FAST);
		sp1.emitNext(2, FAIL_FAST);
		sp1.emitNext(3, FAIL_FAST);

		sp2.emitNext(1, FAIL_FAST);

		sp1.emitNext(4, FAIL_FAST);
		sp1.emitNext(5, FAIL_FAST);

		sp1.emitError(new RuntimeException("forced failure"), FAIL_FAST);

		ts.assertValueCount(2);

		expect(ts, 0, 1, 2, 3);

		toList(ts.values()
		         .get(1)).assertValues(4, 5)
		                 .assertError(RuntimeException.class)
		                 .assertErrorMessage("forced failure")
		                 .assertNotComplete();

		ts.assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		assertThat(sp1.currentSubscriberCount()).as("sp1 has subscriber").isZero();
		assertThat(sp2.currentSubscriberCount()).as("sp2 has subscriber").isZero();
	}

	@Test
	public void otherError() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .window(sp2.asFlux())
		   .subscribe(ts);

		ts.assertValueCount(1);

		sp1.emitNext(1, FAIL_FAST);
		sp1.emitNext(2, FAIL_FAST);
		sp1.emitNext(3, FAIL_FAST);

		sp2.emitNext(1, FAIL_FAST);

		sp1.emitNext(4, FAIL_FAST);
		sp1.emitNext(5, FAIL_FAST);

		sp2.emitError(new RuntimeException("forced failure"), FAIL_FAST);

		ts.assertValueCount(2);

		expect(ts, 0, 1, 2, 3);

		toList(ts.values()
		         .get(1)).assertValues(4, 5)
		                 .assertError(RuntimeException.class)
		                 .assertErrorMessage("forced failure")
		                 .assertNotComplete();

		ts.assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		assertThat(sp1.currentSubscriberCount()).as("sp1 has subscriber").isZero();
		assertThat(sp2.currentSubscriberCount()).as("sp2 has subscriber").isZero();
	}


	Flux<List<Integer>> scenario_windowWillSubdivideAnInputFluxTime() {
		return Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
		           .delayElements(Duration.ofMillis(99))
		           .window(Duration.ofMillis(200))
		           .concatMap(Flux::buffer);
	}

	@Test
	public void windowWillSubdivideAnInputFluxTime() {
		StepVerifier.withVirtualTime(this::scenario_windowWillSubdivideAnInputFluxTime)
		            .thenAwait(Duration.ofSeconds(10))
		            .assertNext(t -> assertThat(t).containsExactly(1, 2))
		            .assertNext(t -> assertThat(t).containsExactly(3, 4))
		            .assertNext(t -> assertThat(t).containsExactly(5, 6))
		            .assertNext(t -> assertThat(t).containsExactly(7, 8))
		            .verifyComplete();
	}

	@Test
	public void windowWillAccumulateMultipleListsOfValues() {
		//given: "a source and a collected flux"
		Sinks.Many<Integer> numbers = Sinks.many().multicast().onBackpressureBuffer();

		//non overlapping buffers
		Sinks.Many<Integer> boundaryFlux = Sinks.many().multicast().onBackpressureBuffer();

		StepVerifier.create(numbers.asFlux()
								   .window(boundaryFlux.asFlux())
								   .concatMap(Flux::buffer)
								   .collectList())
					.then(() -> {
						numbers.emitNext(1, FAIL_FAST);
						numbers.emitNext(2, FAIL_FAST);
						numbers.emitNext(3, FAIL_FAST);
						boundaryFlux.emitNext(1, FAIL_FAST);
						numbers.emitNext(5, FAIL_FAST);
						numbers.emitNext(6, FAIL_FAST);
						numbers.emitComplete(FAIL_FAST);
						//"the collected lists are available"
					})
					.assertNext(res -> assertThat(res).containsExactly(Arrays.asList(1, 2, 3), Arrays.asList(5, 6)))
					.verifyComplete();
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxWindowBoundary<Integer, Integer> test = new FluxWindowBoundary<>(parent, Flux.just(2), Queues.empty());

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    public void scanMainSubscriber() {
        CoreSubscriber<Flux<Integer>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxWindowBoundary.WindowBoundaryMain<Integer, Integer> test = new FluxWindowBoundary.WindowBoundaryMain<>(actual,
        		Queues.unbounded(), Queues.<Integer>unbounded().get());
        Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		Assertions.assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		test.requested = 35;
		Assertions.assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
		test.queue.offer(37);
		Assertions.assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

		Assertions.assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
		test.error = new IllegalStateException("boom");
		Assertions.assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");

		Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onComplete();
		Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanOtherSubscriber() {
        CoreSubscriber<Flux<Integer>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxWindowBoundary.WindowBoundaryMain<Integer, Integer> main = new FluxWindowBoundary.WindowBoundaryMain<>(actual,
        		Queues.unbounded(), Queues.<Integer>unbounded().get());
        FluxWindowBoundary.WindowBoundaryOther<Integer> test =
        		new FluxWindowBoundary.WindowBoundaryOther<>(main);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        test.requested = 35;
        Assertions.assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);

        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }
}
