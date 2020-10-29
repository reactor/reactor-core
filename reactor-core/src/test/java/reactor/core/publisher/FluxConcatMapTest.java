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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class  FluxConcatMapTest extends AbstractFluxConcatMapTest {

	@Override
	int implicitPrefetchValue() {
		return Queues.XS_BUFFER_SIZE;
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Stream.concat(
				super.scenarios_operatorSuccess().stream(),
				Stream.of(
						scenario(f -> f.concatMapDelayError(Flux::just, true, 32)),

						scenario(f -> f.concatMapDelayError(d -> Flux.just(d).hide(), true, 32)),

						scenario(f -> f.concatMapDelayError(d -> Flux.empty(), true, 32))
								.receiverEmpty(),

						scenario(f -> f.concatMap(Flux::just, 1)).prefetch(1),

						//scenarios with fromCallable(() -> null)
						scenario(f -> f.concatMap(d -> Mono.fromCallable(() -> null), 1))
								.prefetch(1)
								.receiverEmpty(),

						scenario(f -> f.concatMapDelayError(d -> Mono.fromCallable(() -> null), true, 32))
								.shouldHitDropErrorHookAfterTerminate(true)
								.receiverEmpty()
				)
		).collect(Collectors.toList());
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Stream.concat(
				super.scenarios_errorFromUpstreamFailure().stream(),
				Stream.of(
						scenario(f -> f.concatMap(Flux::just, 1)).prefetch(1),

						scenario(f -> f.concatMapDelayError(Flux::just, true, 32))
								.shouldHitDropErrorHookAfterTerminate(true)
				)
		).collect(Collectors.toList());
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Stream.concat(
				super.scenarios_operatorError().stream(),
				Stream.of(
						scenario(f -> f.concatMap(d -> {
							throw exception();
						}, 1)).prefetch(1),

						scenario(f -> f.concatMap(d -> null, 1))
								.prefetch(1),

						scenario(f -> f.concatMapDelayError(d -> {
							throw exception();
						}, true, 32))
								.shouldHitDropErrorHookAfterTerminate(true),

						scenario(f -> f.concatMapDelayError(d -> null, true, 32))
								.shouldHitDropErrorHookAfterTerminate(true)
				)
		).collect(Collectors.toList());
	}

	@Test
	public void singleSubscriberOnly() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> source = Sinks.unsafe().many().multicast().directBestEffort();

		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		source.asFlux()
			  .concatMap(v -> v == 1 ? source1.asFlux() : source2.asFlux())
		      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.emitNext(1, FAIL_FAST);
		source.emitNext(2, FAIL_FAST);

		assertThat(source1.currentSubscriberCount()).as("source1 has subscriber").isPositive();
		assertThat(source2.currentSubscriberCount()).as("source2 has subscriber").isZero();

		source1.tryEmitNext(1).orThrow();
		//using an emit below would terminate the sink with an error
		assertThat(source2.tryEmitNext(10))
				.as("early emit in source2")
				.isEqualTo(Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER);

		source1.tryEmitComplete().orThrow();
		source.emitComplete(FAIL_FAST);

		source2.tryEmitNext(2).orThrow();
		source2.tryEmitComplete().orThrow();

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void singleSubscriberOnlyBoundary() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> source = Sinks.unsafe().many().multicast().directBestEffort();

		Sinks.Many<Integer> source1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> source2 = Sinks.unsafe().many().multicast().directBestEffort();

		source.asFlux()
			  .concatMapDelayError(v -> v == 1 ? source1.asFlux() : source2.asFlux())
		      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.emitNext(1, FAIL_FAST);

		assertThat(source1.currentSubscriberCount()).as("source1 has subscriber").isPositive();
		assertThat(source2.currentSubscriberCount()).as("source2 has subscriber").isZero();

		source1.tryEmitNext(1).orThrow();
		//using an emit below would terminate the sink with an error
		assertThat(source2.tryEmitNext(10))
				.as("early emit in source2")
				.isEqualTo(Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER);

		source1.tryEmitComplete().orThrow();
		source.emitNext(2, FAIL_FAST);
		source.emitComplete(FAIL_FAST);

		source2.tryEmitNext(2).orThrow();
		source2.tryEmitComplete().orThrow();

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertComplete();

		assertThat(source1.currentSubscriberCount()).as("source1 has subscribers?").isZero();
		assertThat(source2.currentSubscriberCount()).as("source2 has subscribers?").isZero();
	}

	@Test
	public void allEmptyBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(0, 10)
		    .hide()
		    .concatMap(v -> Flux.<Integer>empty(), 2)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void issue422(){
		Flux<Integer> source = Flux.create((sink) -> {
			for (int i = 0; i < 300; i++) {
				sink.next(i);
			}
			sink.complete();
		});
		Flux<Integer> cached = source.cache();


		long cachedCount = cached.concatMapIterable(Collections::singleton)
		                         .distinct().count().block();

		//System.out.println("source: " + sourceCount);
		System.out.println("cached: " + cachedCount);
	}

	@Test
	public void prefetchMaxTranslatesToUnboundedRequest() {
		AtomicLong requested = new AtomicLong();

		StepVerifier.create(Flux.just(1, 2, 3).hide()
		                        .doOnRequest(requested::set)
		                        .concatMap(i -> Flux.range(0, i), Integer.MAX_VALUE))
		            .expectNext(0, 0, 1, 0, 1, 2)
		            .verifyComplete();

		assertThat(requested.get())
				.isNotEqualTo(Integer.MAX_VALUE)
				.isEqualTo(Long.MAX_VALUE);
	}

	@Test
	public void prefetchMaxTranslatesToUnboundedRequest2() {
		AtomicLong requested = new AtomicLong();

		StepVerifier.create(Flux.just(1, 2, 3).hide()
		                        .doOnRequest(requested::set)
		                        .concatMapDelayError(i -> Flux.range(0, i), Integer.MAX_VALUE))
		            .expectNext(0, 0, 1, 0, 1, 2)
		            .verifyComplete();

		assertThat(requested.get())
				.isNotEqualTo(Integer.MAX_VALUE)
				.isEqualTo(Long.MAX_VALUE);
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1, 2);
		FluxConcatMap<Integer, Integer> test = new FluxConcatMap<>(parent, Flux::just, Queues.one(), Integer.MAX_VALUE, FluxConcatMap.ErrorMode.END);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanConcatMapDelayed() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxConcatMap.ConcatMapDelayed<String, Integer> test = new FluxConcatMap.ConcatMapDelayed<>(
				actual, s -> Mono.just(s.length()), Queues.one(), 123, true);

		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		test.error = new IllegalStateException("boom");
		test.queue.offer("foo");

		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
		assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancelled = true;
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanConcatMapImmediate() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxConcatMap.ConcatMapImmediate<String, Integer> test = new FluxConcatMap.ConcatMapImmediate<>(
				actual, s -> Mono.just(s.length()), Queues.one(), 123);

		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		test.queue.offer("foo");

		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
		assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isFalse();
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onComplete();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancelled = true;
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanConcatMapImmediateError() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxConcatMap.ConcatMapImmediate<String, Integer> test = new FluxConcatMap.ConcatMapImmediate<>(
				actual, s -> Mono.just(s.length()), Queues.one(), 123);

		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isFalse();

		//note that most of the time, the error will be hidden by TERMINATED as soon as it has been propagated downstream :(
		test.error = new IllegalStateException("boom");
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom2"));
		assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(Exceptions.TERMINATED);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanConcatMapInner(){
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxConcatMap.ConcatMapImmediate<String, Integer> parent = new FluxConcatMap.ConcatMapImmediate<>(
				actual, s -> Mono.just(s.length()), Queues.one(), 123);
		FluxConcatMap.ConcatMapInner<Integer> test = new FluxConcatMap.ConcatMapInner<>(parent);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void discardOnNextQueueReject() {
		List<Object> discarded = new ArrayList<>();
		AssertSubscriber<Object> discardSubscriber = new AssertSubscriber<>(
				Context.of(Hooks.KEY_ON_DISCARD, (Consumer<?>) discarded::add));

		final CoreSubscriber<Object> subscriber =
				FluxConcatMap.subscriber(discardSubscriber,
						Mono::just,
						Queues.get(0),
						1,
						FluxConcatMap.ErrorMode.IMMEDIATE);
		subscriber.onSubscribe(Operators.emptySubscription());

		subscriber.onNext(1);

		assertThat(discarded).containsExactly(1);
	}

	@Test
	public void discardOnError() {
		//also tests WeakScalar
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .concatWith(Mono.error(new IllegalStateException("boom")))
		                        .concatMap(i -> Mono.just("value" + i)),
				0)
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly("value1", 2, 3); //"value1" comes from error cancelling the only inner in flight, the 2 other values are still raw in the queue
	}

	@Test
	public void discardOnCancel() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .concatMap(i -> Mono.just("value" + i), implicitPrefetchValue()),
				0)
		            .thenCancel()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1, 2, 3);
	}

	@Test
	public void discardDelayedOnNextQueueReject() {
		List<Object> discarded = new ArrayList<>();
		AssertSubscriber<Object> testSubscriber = new AssertSubscriber<>(
				Context.of(Hooks.KEY_ON_DISCARD, (Consumer<?>) discarded::add));
		final CoreSubscriber<Object> subscriber =
				FluxConcatMap.subscriber(testSubscriber,
						Mono::just,
						Queues.get(0),
						1,
						FluxConcatMap.ErrorMode.END);
		subscriber.onSubscribe(Operators.emptySubscription());

		subscriber.onNext(1);

		assertThat(discarded).containsExactly(1);
	}

	/**
	 * TODO this test overrides the base one because
	 *  FluxConcatMap only discards the first element and not all (bug?)
	 */
	@Test
	@Override
	public void discardDelayedOnDrainMapperError() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .concatMapDelayError(i -> { throw new IllegalStateException("boom"); }))
		            .expectErrorMessage("boom")
		            .verifyThenAssertThat()
		            .hasDiscardedExactly(1);
	}
}
