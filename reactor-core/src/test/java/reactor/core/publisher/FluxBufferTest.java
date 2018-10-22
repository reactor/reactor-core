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
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxBufferTest extends FluxOperatorTest<String, List<String>> {

	@Override
	protected Scenario<String, List<String>> defaultScenarioOptions(Scenario<String, List<String>> defaultOptions) {
		return defaultOptions.shouldAssertPostTerminateState(false);
	}

	@Override
	protected List<Scenario<String, List<String>>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.buffer(Integer.MAX_VALUE, () -> null)),

				scenario(f -> f.buffer(Integer.MAX_VALUE, () -> {
					throw exception();
				})),

				scenario(f -> f.buffer(2, 1, () -> null)),

				scenario(f -> f.buffer(2, 1, () -> {
					throw exception();
				})),

				scenario(f -> f.buffer(1, 2, () -> null)),

				scenario(f -> f.buffer(1, 2, () -> {
					throw exception();
				}))
		);
	}

	@Override
	protected List<Scenario<String, List<String>>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.buffer(1, 2))
						.receive(s -> assertThat(s).containsExactly(item(0)),
								s -> assertThat(s).containsExactly(item(2))),

				scenario(f -> f.buffer(2, 1))
						.receive(s -> assertThat(s).containsExactly(item(0), item(1)),
								s -> assertThat(s).containsExactly(item(1), item(2)),
								s -> assertThat(s).containsExactly(item(2))),

				scenario(f -> f.buffer(1))
						.receive(s -> assertThat(s).containsExactly(item(0)),
								s -> assertThat(s).containsExactly(item(1)),
								s -> assertThat(s).containsExactly(item(2))),

				scenario(Flux::buffer)
						.receive(s -> assertThat(s).containsExactly(item(0), item(1), item(2)))
		);
	}

	@Override
	protected List<Scenario<String, List<String>>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(
				scenario(Flux::buffer),

				scenario(f -> f.buffer(1, 2)),

				scenario(f -> f.buffer(2, 1))
		);
	}

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxBuffer<>(null, 1, ArrayList::new);
	}

	@Test(expected = NullPointerException.class)
	public void supplierNull() {
		Flux.never().buffer(1, 1, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void sizeZero() {
		Flux.never().buffer(0, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void skipZero() {
		Flux.never().buffer(1, 0);
	}

	@Test
	public void normalExact() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();

		Flux.range(1, 10).buffer(2).subscribe(ts);

		ts.assertValues(Arrays.asList(1, 2),
				Arrays.asList(3, 4),
				Arrays.asList(5, 6),
				Arrays.asList(7, 8),
				Arrays.asList(9, 10))
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalExactBackpressured() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).buffer(2).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(Arrays.asList(1, 2), Arrays.asList(3, 4))
		  .assertNoError()
		  .assertNotComplete();

		ts.request(3);

		ts.assertValues(Arrays.asList(1, 2),
				Arrays.asList(3, 4),
				Arrays.asList(5, 6),
				Arrays.asList(7, 8),
				Arrays.asList(9, 10))
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void largerSkip() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();

		Flux.range(1, 10).buffer(2, 3).subscribe(ts);

		ts.assertValues(Arrays.asList(1, 2),
				Arrays.asList(4, 5),
				Arrays.asList(7, 8),
				Arrays.asList(10))
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void largerSkipEven() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();

		Flux.range(1, 8).buffer(2, 3).subscribe(ts);

		ts.assertValues(Arrays.asList(1, 2), Arrays.asList(4, 5), Arrays.asList(7, 8))
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void largerSkipEvenBackpressured() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create(0);

		Flux.range(1, 8).buffer(2, 3).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(Arrays.asList(1, 2), Arrays.asList(4, 5))
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(Arrays.asList(1, 2), Arrays.asList(4, 5), Arrays.asList(7, 8))
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void largerSkipBackpressured() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).buffer(2, 3).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(Arrays.asList(1, 2), Arrays.asList(4, 5))
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(Arrays.asList(1, 2),
				Arrays.asList(4, 5),
				Arrays.asList(7, 8),
				Arrays.asList(10))
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void smallerSkip() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();

		Flux.range(1, 10).buffer(2, 1).subscribe(ts);

		ts.assertValues(Arrays.asList(1, 2),
				Arrays.asList(2, 3),
				Arrays.asList(3, 4),
				Arrays.asList(4, 5),
				Arrays.asList(5, 6),
				Arrays.asList(6, 7),
				Arrays.asList(7, 8),
				Arrays.asList(8, 9),
				Arrays.asList(9, 10),
				Arrays.asList(10))
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void smallerSkipBackpressured() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).buffer(2, 1).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(Arrays.asList(1, 2), Arrays.asList(2, 3))
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(Arrays.asList(1, 2),
				Arrays.asList(2, 3),
				Arrays.asList(3, 4),
				Arrays.asList(4, 5))
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(Arrays.asList(1, 2),
				Arrays.asList(2, 3),
				Arrays.asList(3, 4),
				Arrays.asList(4, 5),
				Arrays.asList(5, 6),
				Arrays.asList(6, 7),
				Arrays.asList(7, 8),
				Arrays.asList(8, 9),
				Arrays.asList(9, 10))
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(Arrays.asList(1, 2),
				Arrays.asList(2, 3),
				Arrays.asList(3, 4),
				Arrays.asList(4, 5),
				Arrays.asList(5, 6),
				Arrays.asList(6, 7),
				Arrays.asList(7, 8),
				Arrays.asList(8, 9),
				Arrays.asList(9, 10),
				Arrays.asList(10))
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void smallerSkip3Backpressured() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).buffer(3, 1).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(Arrays.asList(1, 2, 3), Arrays.asList(2, 3, 4))
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(Arrays.asList(1, 2, 3),
				Arrays.asList(2, 3, 4),
				Arrays.asList(3, 4, 5),
				Arrays.asList(4, 5, 6))
		  .assertNoError()
		  .assertNotComplete();

		ts.request(4);

		ts.assertValues(Arrays.asList(1, 2, 3),
				Arrays.asList(2, 3, 4),
				Arrays.asList(3, 4, 5),
				Arrays.asList(4, 5, 6),
				Arrays.asList(5, 6, 7),
				Arrays.asList(6, 7, 8),
				Arrays.asList(7, 8, 9),
				Arrays.asList(8, 9, 10))
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(Arrays.asList(1, 2, 3),
				Arrays.asList(2, 3, 4),
				Arrays.asList(3, 4, 5),
				Arrays.asList(4, 5, 6),
				Arrays.asList(5, 6, 7),
				Arrays.asList(6, 7, 8),
				Arrays.asList(7, 8, 9),
				Arrays.asList(8, 9, 10),
				Arrays.asList(9, 10))
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(Arrays.asList(1, 2, 3),
				Arrays.asList(2, 3, 4),
				Arrays.asList(3, 4, 5),
				Arrays.asList(4, 5, 6),
				Arrays.asList(5, 6, 7),
				Arrays.asList(6, 7, 8),
				Arrays.asList(7, 8, 9),
				Arrays.asList(8, 9, 10),
				Arrays.asList(9, 10),
				Arrays.asList(10))
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void supplierReturnsNull() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 10).buffer(2, 1, () -> null).subscribe(ts);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void supplierThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 10).buffer(2, 1, () -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void bufferWillSubdivideAnInputFlux() {
		Flux<Integer> numbers = Flux.just(1, 2, 3, 4, 5, 6, 7, 8);

		//"non overlapping buffers"
		List<List<Integer>> res = numbers.buffer(2, 3)
		                                 .buffer()
		                                 .blockLast();

		assertThat(res).containsExactly(Arrays.asList(1, 2),
				Arrays.asList(4, 5),
				Arrays.asList(7, 8));
	}

	@Test
	public void bufferWillSubdivideAnInputFluxOverlap() {
		Flux<Integer> numbers = Flux.just(1, 2, 3, 4, 5, 6, 7, 8);

		//"non overlapping buffers"
		List<List<Integer>> res = numbers.buffer(3, 2)
		                                 .buffer()
		                                 .blockLast();

		assertThat(res).containsExactly(
				Arrays.asList(1, 2, 3),
				Arrays.asList(3, 4, 5),
				Arrays.asList(5, 6, 7),
				Arrays.asList(7, 8));
	}

	@Test
	public void bufferWillRerouteAsManyElementAsSpecified() {
		assertThat(Flux.just(1, 2, 3, 4, 5)
		               .buffer(2)
		               .collectList()
		               .block()).containsExactly(Arrays.asList(1, 2),
				Arrays.asList(3, 4),
				Arrays.asList(5));
	}

	@Test
	public void scanExactSubscriber() {
		CoreSubscriber<? super List> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxBuffer.BufferExactSubscriber<String, List<String>> test = new FluxBuffer.BufferExactSubscriber<>(
					actual, 23, ArrayList::new	);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);
		test.onNext("foo");

		assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(23);
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(23);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanOverlappingSubscriber() {
		CoreSubscriber<? super List> actual = new LambdaSubscriber<>(null, e -> {
		}, null, null);
		FluxBuffer.BufferOverlappingSubscriber<String, List<String>> test =
				new FluxBuffer.BufferOverlappingSubscriber<>(actual, 23, 2, ArrayList::new);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);
		test.onNext("foo");
		test.onNext("bar");
		assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(23);
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(2);
		test.onNext("baz");
		assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(46); //2 buffers
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(4); // buffered foo bar baz then baz

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(Long.MAX_VALUE);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanOverlappingSubscriberCancelled() {
		CoreSubscriber<? super List>
				actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxBuffer.BufferOverlappingSubscriber<String, List<String>> test = new FluxBuffer.BufferOverlappingSubscriber<>(
				actual, 23, 5, ArrayList::new);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanSkipSubscriber() {
		CoreSubscriber<? super List> actual = new LambdaSubscriber<>(null, e -> {}, null, null);

		FluxBuffer.BufferSkipSubscriber<String, List<String>> test = new FluxBuffer.BufferSkipSubscriber<>(actual, 2, 3, ArrayList::new);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
		test.onNext("foo");
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
		test.onNext("bar");
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(0); //buffer emitted
		test.onNext("drop");
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(0); //buffer not replenished

		assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(2);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(2);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void discardOnCancel() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .concatWith(Mono.never())
		                        .buffer(4))
		            .thenAwait(Duration.ofMillis(10))
		            .thenCancel()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2, 3);
	}

	@Test
	public void discardOnCancelSkip() {
		StepVerifier.create(Flux.just(1, 2, 3, 4, 5)
		                        .limitRequest(2)
		                        .concatWith(Mono.never())
		                        .buffer(3, 4))
		            .thenAwait(Duration.ofMillis(10))
		            .thenCancel()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2);
	}

	@Test
	public void discardOnCancelOverlap() {
		StepVerifier.create(Flux.just(1, 2, 3, 4, 5, 6)
		                        .limitRequest(2)
		                        .concatWith(Mono.never())
		                        .buffer(4, 2))
		            .thenAwait(Duration.ofMillis(10))
		            .thenCancel()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2);
	}

	@Test
	public void discardOnNextSupplierError() {
		Supplier<List<Integer>> bufferSupplier = () -> null;

		StepVerifier.create(Flux.just(1, 2, 3)
		                        .buffer(4, 4, bufferSupplier))
		            .expectErrorMessage("The bufferSupplier returned a null buffer")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1);
	}

	@Test
	public void discardOnNextSupplierErrorSkip() {
		Supplier<List<Integer>> bufferSupplier = () -> null;

		StepVerifier.create(Flux.just(1, 2, 3, 4, 5)
		                        .buffer(3, 4, bufferSupplier))
		            .expectErrorMessage("The bufferSupplier returned a null buffer")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1);
	}

	@Test
	public void discardOnNextSupplierErrorOverlap() {
		Supplier<List<Integer>> bufferSupplier = () -> null;

		StepVerifier.create(Flux.just(1, 2, 3, 4, 5, 6)
		                        .buffer(4, 2, bufferSupplier))
		            .expectErrorMessage("The bufferSupplier returned a null buffer")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1);
	}

	@Test
	public void discardOnSkippedElements() {
		//the skip flavor should discard elements that are not added to any buffer
		StepVerifier.create(Flux.just(1, 2, 3, 4, 5)
		                        .buffer(2, 3)
		                        .flatMapIterable(Function.identity()))
		            .expectNext(1, 2, 4, 5)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(3);
	}

	@Test
	public void discardOnError() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .concatWith(Mono.error(new IllegalStateException("boom")))
		                        .buffer(4))
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2, 3);
	}

	@Test
	public void discardOnErrorSkip() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .concatWith(Mono.error(new IllegalStateException("boom")))
		                        .buffer(4, 5))
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2, 3);
	}

	@Test
	public void discardOnErrorOverlap() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .concatWith(Mono.error(new IllegalStateException("boom")))
		                        .buffer(4, 2))
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2, 3, 3); //we already opened a 2nd buffer
	}
}
