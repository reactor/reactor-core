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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.assertj.core.api.Condition;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.concurrent.Queues;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxBufferWhenTest {

	private static final Logger LOGGER = Loggers.getLogger(FluxBufferWhenTest.class);

	@Test
	public void gh969_timedOutBuffersDontLeak() throws InterruptedException {
		LongAdder created = new LongAdder();
		LongAdder finalized = new LongAdder();
		class Wrapper {

			final int i;

			Wrapper(int i) {
				created.increment();
				this.i = i;
			}

			@Override
			public String toString() {
				return "{i=" + i + '}';
			}

			@Override
			protected void finalize() {
				finalized.increment();
			}
		}

		final CountDownLatch latch = new CountDownLatch(1);
		final UnicastProcessor<Wrapper> processor = UnicastProcessor.create();

		Flux<Integer> emitter = Flux.range(1, 400)
		                            .delayElements(Duration.ofMillis(10))
		                            .doOnNext(i -> processor.onNext(new Wrapper(i)))
		                            .doOnError(processor::onError)
		                            .doOnComplete(processor::onComplete);

		Mono<List<Tuple3<Long, String, Long>>> buffers =
				processor.buffer(Duration.ofMillis(1000), Duration.ofMillis(500))
				         .filter(b -> b.size() > 0)
				         .index()
				         .doOnNext(it -> System.gc())
				         //index, bounds of buffer, finalized
				         .map(t2 -> Tuples.of(t2.getT1(),
						         String.format("from %s to %s", t2.getT2().get(0),
								         t2.getT2().get(t2.getT2().size() - 1)),
						         finalized.longValue()))
				         .doOnNext(v -> LOGGER.info(v.toString()))
				         .doOnComplete(latch::countDown)
				         .collectList();

		emitter.subscribe();
		List<Tuple3<Long, String, Long>> finalizeStats = buffers.block(Duration.ofSeconds(10));

		Condition<? super Tuple3<Long, String, Long>> hasFinalized = new Condition<>(
				t3 -> t3.getT3() > 0, "has finalized");

		//at least 5 intermediate finalize
		assertThat(finalizeStats).areAtLeast(5, hasFinalized);

		latch.await(10, TimeUnit.SECONDS);
		LOGGER.debug("final GC");
		System.gc();
		Thread.sleep(500);

		assertThat(finalized.longValue())
				.as("final GC collects all")
				.isEqualTo(created.longValue());
	}

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

		ts.assertValues(Arrays.asList(2, 3, 4), Collections.singletonList(6))
		  .assertNoError()
		  .assertNotComplete();

		sp1.onComplete();

		ts.assertValues(Arrays.asList(2, 3, 4), Collections.singletonList(6))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void startCompletes() {
		AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();

		DirectProcessor<Integer> source = DirectProcessor.create();
		DirectProcessor<Integer> open = DirectProcessor.create();
		DirectProcessor<Integer> close = DirectProcessor.create();

		source.bufferWhen(open, v -> close)
		   .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.onNext(1);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		open.onNext(1);
		open.onComplete();

		Assert.assertTrue("close has no subscribers?", close.hasDownstreams());

		source.onNext(2);
		source.onNext(3);
		source.onNext(4);

		close.onComplete();

		ts.assertValues(Arrays.asList(2, 3, 4))
		  .assertNoError()
		  .assertComplete();

//		Assert.assertFalse("source has subscribers?", source.hasDownstreams()); //FIXME
		Assert.assertFalse("open has subscribers?", open.hasDownstreams());
		Assert.assertFalse("close has subscribers?", close.hasDownstreams());

	}


	@Test
	public void bufferWillAcumulateMultipleListsOfValuesOverlap() {
		//given: "a source and a collected flux"
		EmitterProcessor<Integer> numbers = EmitterProcessor.create();
		EmitterProcessor<Integer> bucketOpening = EmitterProcessor.create();

		//"overlapping buffers"
		EmitterProcessor<Integer> boundaryFlux = EmitterProcessor.create();

		MonoProcessor<List<List<Integer>>> res = numbers.bufferWhen(bucketOpening, u -> boundaryFlux )
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
		assertThat(res.block()).containsExactly(Arrays.asList(3, 5),
				Collections.singletonList(5), Collections.emptyList());
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

	@Test
	public void scanStartEndMain() {
		CoreSubscriber<List<String>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);

		FluxBufferWhen.BufferWhenMainSubscriber<String, Integer, Long, List<String>> test =
				new FluxBufferWhen.BufferWhenMainSubscriber<String, Integer, Long, List<String>>(
						actual, ArrayList::new, u -> Mono.just(1L));
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);
		test.request(100L);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(0); //TODO
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(100L);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanStartEndMainCancelled() {
		CoreSubscriber<List<String>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);

		FluxBufferWhen.BufferWhenMainSubscriber<String, Integer, Long, List<String>> test =
				new FluxBufferWhen.BufferWhenMainSubscriber<String, Integer, Long, List<String>>(
				actual, ArrayList::new, u -> Mono.just(1L));
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanStartEndMainCompleted() {
		CoreSubscriber<List<String>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);

		FluxBufferWhen.BufferWhenMainSubscriber<String, Integer, Long, List<String>> test =
				new FluxBufferWhen.BufferWhenMainSubscriber<String, Integer, Long, List<String>>(
				actual, ArrayList::new, u -> Mono.just(1L));
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();

		test.complete();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}


	@Test
	public void scanWhenCloseSubscriber() {
		//noinspection ConstantConditions
		FluxBufferWhen.BufferWhenMainSubscriber<String, Integer, Long, List<String>> main =
				new FluxBufferWhen.BufferWhenMainSubscriber<>(null,
						ArrayList::new,
						u -> Mono.just(1L));

		FluxBufferWhen.BufferWhenCloseSubscriber test = new FluxBufferWhen.BufferWhenCloseSubscriber<>(Arrays.asList("foo", "bar"), main);

		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(Long.MAX_VALUE);

		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.dispose();
		assertThat(test.isDisposed()).isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanWhenOpenSubscriber() {
		//noinspection ConstantConditions
		FluxBufferWhen.BufferWhenMainSubscriber<String, Integer, Long, List<String>> main = new FluxBufferWhen.BufferWhenMainSubscriber<>(
				null, ArrayList::new, u -> Mono.just(1L));

		FluxBufferWhen.BufferWhenOpenSubscriber test = new FluxBufferWhen.BufferWhenOpenSubscriber<>(main);


		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(Long.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.dispose();
		assertThat(test.isDisposed()).isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}
}
