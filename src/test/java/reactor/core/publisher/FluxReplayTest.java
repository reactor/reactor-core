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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.fail;

public class FluxReplayTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.prefetch(Integer.MAX_VALUE)
				.shouldAssertPostTerminateState(false);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.replay().autoConnect()),

				scenario(f -> f.replay().refCount())
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_touchAndAssertState() {
		return Arrays.asList(
				scenario(f -> f.replay().autoConnect())
		);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failPrefetch(){
		Flux.never()
		    .replay( -1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failTime(){
		Flux.never()
		    .replay( Duration.ofDays(-1));
	}

	VirtualTimeScheduler vts;

	@Before
	public void vtsStart() {
		//delayElements (notably) now uses parallel() so VTS must be enabled everywhere
		vts = VirtualTimeScheduler.getOrSet();
	}

	@After
	public void vtsStop() {
		vts = null;
		VirtualTimeScheduler.reset();
	}

	@Test
	public void cacheFlux() {

		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis(1000))
		                                         .replay()
		                                         .autoConnect()
		                                         .elapsed();

		StepVerifier.create(source)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.create(source)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();

	}

	@Test
	public void cacheFluxFused() {

		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis(1000))
		                                         .replay()
		                                         .autoConnect()
		                                         .elapsed();

		StepVerifier.create(source)
		            .expectFusion(Fuseable.ANY)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.create(source)
		            .expectFusion(Fuseable.ANY)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();

	}

	@Test
	public void cacheFluxTTL() {

		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis(1000))
		                                         .replay(Duration.ofMillis(2000))
		                                         .autoConnect()
		                                         .elapsed();

		StepVerifier.create(source)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.create(source)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();

	}

	@Test
	public void cacheFluxTTLFused() {

		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis(1000))
		                                         .replay(Duration.ofMillis(2000))
		                                         .autoConnect()
		                                         .elapsed();

		StepVerifier.create(source)
		            .expectFusion(Fuseable.ANY)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.create(source)
		            .expectFusion(Fuseable.ANY)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();

	}

	@Test
	public void cacheFluxTTLMillis() {

		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis(1000))
		                                         .replay(Duration.ofMillis(2000), vts)
		                                         .autoConnect()
		                                         .elapsed();

		StepVerifier.create(source)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.create(source)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();

	}

	@Test
	public void cacheFluxHistoryTTL() {

		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis(1000))
		                                         .replay(2, Duration.ofMillis(2000))
		                                         .autoConnect()
		                                         .elapsed();

		StepVerifier.create(source)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.create(source)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();

	}

	@Test
	public void cacheFluxHistoryTTLFused() {

		Flux<Tuple2<Long, Integer>> source = Flux.just(1, 2, 3)
		                                         .delayElements(Duration.ofMillis(1000))
		                                         .replay(2, Duration.ofMillis(2000))
		                                         .autoConnect()
		                                         .elapsed();

		StepVerifier.create(source)
		            .expectFusion(Fuseable.ANY)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 1)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 1000 && t.getT2() == 3)
		            .verifyComplete();

		StepVerifier.create(source)
		            .expectFusion(Fuseable.ANY)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(3)))
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 2)
		            .expectNextMatches(t -> t.getT1() == 0 && t.getT2() == 3)
		            .verifyComplete();
	}

	@Test
	public void cancel() {
		ConnectableFlux<Integer> replay = Flux.range(1, 5)
		                                             .replay(2);
		Disposable subscribed = replay.subscribe(v -> {},
				e -> fail(e.toString()));

		Disposable connected = replay.connect();

		//the lambda subscriber itself is cancelled so it will bubble the exception
		//propagated by connect().dipose()
		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(connected::dispose)
	            .withMessage("java.util.concurrent.CancellationException: Disconnected");

		boolean cancelled = ((FluxReplay.ReplaySubscriber) connected).cancelled;
		assertThat(cancelled).isTrue();
	}

	@Test
    public void scanMain() {
        Flux<Integer> parent = Flux.just(1);
        FluxReplay<Integer> test = new FluxReplay<>(parent, 25, 1000, Schedulers.single());

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(25);
    }

	@Test
    public void scanInner() {
		Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxReplay<Integer> main = new FluxReplay<>(Flux.just(1), 2, 1000, Schedulers.single());
        FluxReplay.ReplayInner<Integer> test = new FluxReplay.ReplayInner<>(actual);
        FluxReplay.ReplaySubscriber<Integer> parent = new FluxReplay.ReplaySubscriber<>(new FluxReplay.UnboundedReplayBuffer<>(10), main);
        parent.trySubscribe(test);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
        Assertions.assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(0); // RS: TODO non-zero size
        test.request(35);
        Assertions.assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.parent.terminate();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }

	@Test
    public void scanSubscriber() {
		FluxReplay<Integer> parent = new FluxReplay<>(Flux.just(1), 2, 1000, Schedulers.single());
        FluxReplay.ReplaySubscriber<Integer> test = new FluxReplay.ReplaySubscriber<>(new FluxReplay.UnboundedReplayBuffer<>(10), parent);
        Subscription sub = Operators.emptySubscription();
        test.onSubscribe(sub);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(sub);
        Assertions.assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
        Assertions.assertThat(test.scan(Scannable.IntAttr.CAPACITY)).isEqualTo(Integer.MAX_VALUE);
        test.buffer.add(1);
        Assertions.assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(1);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        Assertions.assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isNull();
        test.onError(new IllegalStateException("boom"));
        Assertions.assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");
        test.terminate();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        test.cancelled = true;
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }
}