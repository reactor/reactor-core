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

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.window(sp2)
		   .subscribe(ts);

		ts.assertValueCount(1);

		sp1.onNext(1);
		sp1.onNext(2);
		sp1.onNext(3);

		sp2.onNext(1);

		sp1.onNext(4);
		sp1.onNext(5);

		sp1.onComplete();

		ts.assertValueCount(2);

		expect(ts, 0, 1, 2, 3);
		expect(ts, 1, 4, 5);

		ts.assertNoError()
		  .assertComplete();

		Assert.assertFalse("sp1 has subscribers", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers", sp1.hasDownstreams());
	}

	@Test
	public void normalOtherCompletes() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.window(sp2)
		   .subscribe(ts);

		ts.assertValueCount(1);

		sp1.onNext(1);
		sp1.onNext(2);
		sp1.onNext(3);

		sp2.onNext(1);

		sp1.onNext(4);
		sp1.onNext(5);

		sp2.onComplete();

		ts.assertValueCount(2);

		expect(ts, 0, 1, 2, 3);
		expect(ts, 1, 4, 5);

		ts.assertNoError()
		  .assertComplete();

		Assert.assertFalse("sp1 has subscribers", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers", sp1.hasDownstreams());
	}

	@Test
	public void mainError() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.window(sp2)
		   .subscribe(ts);

		ts.assertValueCount(1);

		sp1.onNext(1);
		sp1.onNext(2);
		sp1.onNext(3);

		sp2.onNext(1);

		sp1.onNext(4);
		sp1.onNext(5);

		sp1.onError(new RuntimeException("forced failure"));

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

		Assert.assertFalse("sp1 has subscribers", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers", sp1.hasDownstreams());
	}

	@Test
	public void otherError() {
		AssertSubscriber<Flux<Integer>> ts = AssertSubscriber.create();

		DirectProcessor<Integer> sp1 = DirectProcessor.create();
		DirectProcessor<Integer> sp2 = DirectProcessor.create();

		sp1.window(sp2)
		   .subscribe(ts);

		ts.assertValueCount(1);

		sp1.onNext(1);
		sp1.onNext(2);
		sp1.onNext(3);

		sp2.onNext(1);

		sp1.onNext(4);
		sp1.onNext(5);

		sp2.onError(new RuntimeException("forced failure"));

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

		Assert.assertFalse("sp1 has subscribers", sp1.hasDownstreams());
		Assert.assertFalse("sp2 has subscribers", sp1.hasDownstreams());
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
	public void windowWillAcumulateMultipleListsOfValues() {
		//given: "a source and a collected flux"
		EmitterProcessor<Integer> numbers = EmitterProcessor.create();

		//non overlapping buffers
		EmitterProcessor<Integer> boundaryFlux = EmitterProcessor.create();

		MonoProcessor<List<List<Integer>>> res = numbers.window(boundaryFlux)
		                                       .concatMap(Flux::buffer)
		                                       .buffer()
		                                       .publishNext()
		                                       .toProcessor();
		res.subscribe();

		numbers.onNext(1);
		numbers.onNext(2);
		numbers.onNext(3);
		boundaryFlux.onNext(1);
		numbers.onNext(5);
		numbers.onNext(6);
		numbers.onComplete();

		//"the collected lists are available"
		assertThat(res.block()).containsExactly(Arrays.asList(1, 2, 3), Arrays.asList(5, 6));
	}

	@Test
    public void scanMainSubscriber() {
        Subscriber<Flux<Integer>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxWindowBoundary.WindowBoundaryMain<Integer, Integer> test = new FluxWindowBoundary.WindowBoundaryMain<>(actual,
        		QueueSupplier.unbounded(), QueueSupplier.<Integer>unbounded().get(), QueueSupplier.unbounded().get());
        Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
		Assertions.assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		test.requested = 35;
		Assertions.assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
		test.queue.offer(37);
		Assertions.assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(1);

		Assertions.assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isNull();
		test.error = new IllegalStateException("boom");
		Assertions.assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");

		Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.onComplete();
		Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

		Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }

	@Test
    public void scanOtherSubscriber() {
        Subscriber<Flux<Integer>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxWindowBoundary.WindowBoundaryMain<Integer, Integer> main = new FluxWindowBoundary.WindowBoundaryMain<>(actual,
        		QueueSupplier.unbounded(), QueueSupplier.<Integer>unbounded().get(), QueueSupplier.unbounded().get());
        FluxWindowBoundary.WindowBoundaryOther<Integer> test =
        		new FluxWindowBoundary.WindowBoundaryOther<>(main);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(main);
        test.requested = 35;
        Assertions.assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }
}
