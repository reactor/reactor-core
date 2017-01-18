/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Test;
import reactor.core.Exceptions;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.TimedScheduler;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class MonoDelayElementTest {

	@After
	public void reset() {
		//TODO remove once the StepVerifier explicitly resets VirtualTimeScheduler
		//see https://github.com/reactor/reactor-addons/issues/70
		VirtualTimeScheduler.reset();
	}

	@Test
	public void normalIsDelayed() {
		Mono<String> source = Mono.just("foo").log().hide();

		StepVerifier.withVirtualTime(() -> new MonoDelayElement<>(source, 2, TimeUnit.SECONDS,
				Schedulers.timer()).log())
	                .expectSubscription()
	                .expectNoEvent(Duration.ofSeconds(2))
	                .expectNext("foo")
	                .verifyComplete();
	}

	@Test
	public void cancelDuringDelay() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		AtomicBoolean emitted = new AtomicBoolean();
		AtomicBoolean cancelled = new AtomicBoolean();
		Mono<String> source = Mono.just("foo").log().hide();

		StepVerifier.withVirtualTime(
				() -> new MonoDelayElement<>(source, 2, TimeUnit.SECONDS, vts)
						.doOnCancel(() -> cancelled.set(true))
						.log()
						.doOnNext(n -> emitted.set(true)),
				() -> vts, Long.MAX_VALUE)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(1))
		            .thenCancel()
		            .verify();

		vts.advanceTimeBy(Duration.ofHours(1));
		assertThat(emitted.get()).isFalse();
		assertThat(cancelled.get()).isTrue();
	}

	@Test
	public void cancelBeforeNext() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		AtomicBoolean emitted = new AtomicBoolean();
		AtomicBoolean cancelled = new AtomicBoolean();

		Mono<Long> source = Mono.delayMillis(1000, vts);

		StepVerifier.withVirtualTime(
				() -> new MonoDelayElement<>(source, 2, TimeUnit.SECONDS, vts)
						.doOnCancel(() -> cancelled.set(true))
						.doOnNext(n -> emitted.set(true)),
				() -> vts, Long.MAX_VALUE)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(500))
		            .thenCancel()
		            .verify();

		vts.advanceTimeBy(Duration.ofHours(1));
		assertThat(emitted.get()).isFalse();
		assertThat(cancelled.get()).isTrue();
	}

	@Test(timeout = 5000L)
	public void emptyIsImmediate() {
		Mono<String> source = Mono.<String>empty().log().hide();

		Duration d = StepVerifier.create(new MonoDelayElement<>(source, 10, TimeUnit.SECONDS,
				Schedulers.timer()).log())
		            .expectSubscription()
		            .verifyComplete();

		assertThat(d).isLessThan(Duration.ofSeconds(1));
	}

	@Test
	public void errorIsImmediate() {
		Mono<String> source = Mono.<String>error(new IllegalStateException("boom")).hide();

		Duration d = StepVerifier.create(new MonoDelayElement<>(source, 10, TimeUnit.SECONDS, Schedulers.timer()).log())
		                         .expectSubscription()
		                         .verifyErrorMessage("boom");

		assertThat(d).isLessThan(Duration.ofSeconds(1));
	}

	@Test
	public void errorAfterNextIsNeverTriggered() {
		TestPublisher<String> source = TestPublisher.create();
		AtomicReference<Throwable> errorDropped = new AtomicReference<>();
		Hooks.onErrorDropped(errorDropped::set);

		try {
			StepVerifier.withVirtualTime(() ->
					new MonoDelayElement<>(source, 2, TimeUnit.SECONDS, Schedulers.timer()))
			            .expectSubscription()
			            .then(() -> source.next("foo").error(new IllegalStateException("boom")))
			            .expectNoEvent(Duration.ofSeconds(2))
			            .expectNext("foo")
			            .verifyComplete();
		} finally {
			Hooks.resetOnErrorDropped();
		}

		assertThat(errorDropped.get()).isNull();
	}

	@Test
	public void onNextOnDisposedSchedulerThrows() {
		TimedScheduler scheduler = Schedulers.newTimer("onNextOnDisposedSchedulerThrows");
		scheduler.dispose();
		Mono<String> source = Mono.just("foo").hide();

		try {
			StepVerifier.create(new MonoDelayElement<>(source, 2, TimeUnit.SECONDS, scheduler))
			            .expectSubscription()
			            .verifyComplete(); //complete not relevant
			fail("expected exception here");
		}
		catch (Throwable e) {
			Throwable t = Exceptions.unwrap(e);

			assertThat(t).isNotEqualTo(e)
		                 .isInstanceOf(RejectedExecutionException.class)
		                 .hasMessage("Scheduler unavailable");

			assertThat(e).satisfies(Exceptions::isBubbling);
		}
	}

	@Test
	public void cancelUpstreamOnceWhenCancelled() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		AtomicLong upstreamCancelCount = new AtomicLong();

		Mono<String> source = Mono.just("foo").log().hide()
				.doOnCancel(() -> upstreamCancelCount.incrementAndGet());

		StepVerifier.withVirtualTime(
				() -> new MonoDelayElement<>(source, 2, TimeUnit.SECONDS, vts),
				() -> vts, Long.MAX_VALUE)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(1))
		            .thenCancel()
		            .verify();

		vts.advanceTimeBy(Duration.ofHours(1));
		assertThat(upstreamCancelCount.get()).isEqualTo(1);
	}

	@Test
	public void cancelUpstreamOnceWhenRejected() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		vts.dispose();
		AtomicLong upstreamCancelCount = new AtomicLong();

		Mono<String> source = Mono.just("foo").log().hide()
		                          .doOnCancel(upstreamCancelCount::incrementAndGet);

		try {
			StepVerifier.withVirtualTime(
					() -> new MonoDelayElement<>(source, 2, TimeUnit.SECONDS, vts).log(),
					() -> vts, Long.MAX_VALUE)
			            .expectSubscription()
			            .verifyComplete();
		}
		catch (Throwable e) {
			assertThat(e).hasMessageContaining("Scheduler unavailable");
		}
		finally {
			assertThat(upstreamCancelCount.get()).isEqualTo(1);
		}
	}

	@Test
	public void monoApiTestDuration() {
		StepVerifier.withVirtualTime(() -> Mono.just("foo").delayElement(Duration.ofHours(1)))
	                .expectSubscription()
	                .expectNoEvent(Duration.ofHours(1))
	                .expectNext("foo")
	                .verifyComplete();
	}

	@Test
	public void monoApiTestMillis() {
		StepVerifier.withVirtualTime(() -> Mono.just("foo").delayElementMillis(5000L))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(5))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void monoApiTestMillisAndTimer() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();

		StepVerifier.withVirtualTime(
				() -> Mono.just("foo").delayElementMillis(5000L, vts),
				() -> vts, Long.MAX_VALUE)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(5))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void guardedAgainstMultipleOnNext() {
		AtomicReference<Object> dropped = new AtomicReference<>();
		Hooks.onNextDropped(dropped::set);

		Flux<String> source = Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext("foo");
			s.onNext("bar");
			s.onComplete();
		});

		try {
			StepVerifier.withVirtualTime(() -> new MonoDelayElement<>(source,
					2,
					TimeUnit.SECONDS,
					Schedulers.timer()))
			            .expectSubscription()
			            .expectNoEvent(Duration.ofSeconds(2))
			            .expectNext("foo")
			            .verifyComplete();
		}
		finally {
			Hooks.resetOnNextDropped();
		}
		assertThat(dropped.get()).isEqualTo("bar");
	}

	@Test
	public void guardedAgainstOnComplete() {
		AtomicReference<Object> dropped = new AtomicReference<>();
		Hooks.onNextDropped(dropped::set);

		Flux<String> source = Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext("foo");
			s.onComplete();
		});

		try {
			StepVerifier.withVirtualTime(() -> new MonoDelayElement<>(source,
					2,
					TimeUnit.SECONDS,
					Schedulers.timer()))
			            .expectSubscription()
			            .expectNoEvent(Duration.ofSeconds(2))
			            .expectNext("foo")
			            .verifyComplete();
		}
		finally {
			Hooks.resetOnNextDropped();
		}
		assertThat(dropped.get()).isEqualTo("bar");
	}

	@Test
	public void guardedAgainstOnError() {
		AtomicReference<Throwable> dropped = new AtomicReference<>();
		Hooks.onErrorDropped(dropped::set);

		Flux<String> source = Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext("foo");
			s.onError(new IllegalStateException("boom"));
		});

		try {
			StepVerifier.withVirtualTime(() -> new MonoDelayElement<>(source,
					2,
					TimeUnit.SECONDS,
					Schedulers.timer()))
			            .expectSubscription()
			            .expectNoEvent(Duration.ofSeconds(2))
			            .expectNext("foo")
			            .verifyComplete();
		}
		finally {
			Hooks.resetOnErrorDropped();
		}
		assertThat(dropped.get()).hasMessage("boom")
		                         .isInstanceOf(IllegalStateException.class);
	}

	@Test
	public void upstreamIsDelayedSource() {
		AtomicReference<Object> upstream = new AtomicReference<>();
		Flux<Integer> source = Flux.range(1, 5);


		StepVerifier.withVirtualTime(() -> new MonoDelayElement<>(source, 2, TimeUnit.SECONDS, Schedulers.timer())
				.doOnSubscribe(s -> {
					assertThat(s).isInstanceOf(MonoDelayElement.MonoDelayElementSubscriber.class);

					MonoDelayElement.MonoDelayElementSubscriber delayedSubscriber =
							(MonoDelayElement.MonoDelayElementSubscriber) s;

					upstream.set(delayedSubscriber.upstream());
				}))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext(1)
		            .verifyComplete();

		assertThat(upstream.get()).isInstanceOf(FluxRange.RangeSubscription.class);
	}
}