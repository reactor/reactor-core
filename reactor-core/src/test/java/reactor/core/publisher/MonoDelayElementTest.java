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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class MonoDelayElementTest {

	private Scheduler defaultSchedulerForDelay() {
		return Schedulers.parallel(); //reflects the default used in Mono.delay(duration)
	}

	@Test
	public void normalIsDelayed() {
		Mono<String> source = Mono.just("foo").log().hide();

		StepVerifier.withVirtualTime(() -> new MonoDelayElement<>(source, 2, TimeUnit.SECONDS,
				defaultSchedulerForDelay()).log())
	                .expectSubscription()
	                .expectNoEvent(Duration.ofSeconds(2))
	                .expectNext("foo")
	                .verifyComplete();
	}

	@Test
	public void subMillisDelay() {
		Mono<String> source = Mono.just("foo");

		StepVerifier.withVirtualTime(() -> source.delayElement(Duration.ofNanos(5000L)).log())
	                .expectSubscription()
	                .expectNoEvent(Duration.ofNanos(5000L))
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

		Mono<Long> source = Mono.delay(Duration.ofMillis(1000), vts);

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

	@Test
	@Timeout(5)
	public void emptyIsImmediate() {
		Mono<String> source = Mono.<String>empty().log().hide();

		Duration d = StepVerifier.create(new MonoDelayElement<>(source, 10, TimeUnit.SECONDS,
				defaultSchedulerForDelay()).log())
		            .expectSubscription()
		            .verifyComplete();

		assertThat(d).isLessThan(Duration.ofSeconds(1));
	}

	@Test
	public void errorIsImmediate() {
		Mono<String> source = Mono.<String>error(new IllegalStateException("boom")).hide();

		Duration d = StepVerifier.create(new MonoDelayElement<>(source, 10, TimeUnit.SECONDS, defaultSchedulerForDelay()).log())
		                         .expectSubscription()
		                         .verifyErrorMessage("boom");

		assertThat(d).isLessThan(Duration.ofSeconds(1));
	}

	@Test
	public void errorAfterNextIsNeverTriggered() {
		TestPublisher<String> source = TestPublisher.create();
		AtomicReference<Throwable> errorDropped = new AtomicReference<>();
		Hooks.onErrorDropped(errorDropped::set);

		StepVerifier.withVirtualTime(() ->
				new MonoDelayElement<>(source.mono(), 2, TimeUnit.SECONDS, defaultSchedulerForDelay()))
		            .expectSubscription()
		            .then(() -> source.next("foo").error(new IllegalStateException("boom")))
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext("foo")
		            .verifyComplete();

		assertThat(errorDropped.get()).isNull();
	}

	@Test
	public void onNextOnDisposedSchedulerThrows() {
		VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
		scheduler.dispose();
		Mono<String> source = Mono.just("foo").hide();

		StepVerifier.create(new MonoDelayElement<>(source, 2, TimeUnit.SECONDS, scheduler))
		            .expectErrorSatisfies(e -> {
		            	assertThat(e)
					            .isInstanceOf(RejectedExecutionException.class)
					            .hasMessage("Scheduler unavailable");
		            });
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
		assertThat(upstreamCancelCount).hasValue(1);
	}

	@Test
	public void cancelUpstreamOnceWhenRejected() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		vts.dispose();

		TestPublisher<Object> testPublisher = TestPublisher.createCold();
		testPublisher.emit("Hello");

		StepVerifier.create(new MonoDelayElement<>(testPublisher.mono(), 2, TimeUnit.SECONDS, vts))
		            .verifyErrorSatisfies(e -> {
			            assertThat(e)
					            .isInstanceOf(RejectedExecutionException.class)
					            .hasMessage("Scheduler unavailable");
		            });

		testPublisher.assertWasRequested();
		testPublisher.assertCancelled(1);
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
		StepVerifier.withVirtualTime(() -> Mono.just("foo").delayElement(Duration.ofMillis(5000L)))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(5))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void monoApiTestMillisAndTimer() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();

		StepVerifier.withVirtualTime(
				() -> Mono.just("foo").delayElement(Duration.ofMillis(5000L), vts),
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

		Mono<String> source = Mono.fromDirect(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext("foo");
			s.onNext("bar");
			s.onComplete();
		});

		try {
			StepVerifier.withVirtualTime(() -> new MonoDelayElement<>(source,
					2,
					TimeUnit.SECONDS,
					defaultSchedulerForDelay()))
			            .expectSubscription()
			            .expectNoEvent(Duration.ofSeconds(2))
			            .expectNext("foo")
			            .verifyComplete();
		}
		finally {
			Hooks.resetOnNextDropped();
		}
		assertThat(dropped).hasValue("bar");
	}

	@Test
	public void guardedAgainstOnComplete() {
		Mono<String> source = Mono.fromDirect(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext("foo");
			s.onComplete();
		});

		StepVerifier.withVirtualTime(() -> new MonoDelayElement<>(source,
				2,
				TimeUnit.SECONDS,
				defaultSchedulerForDelay()))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void guardedAgainstOnError() {
		AtomicReference<Throwable> dropped = new AtomicReference<>();
		Hooks.onErrorDropped(dropped::set);

		Mono<String> source = Mono.fromDirect(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext("foo");
			s.onError(new IllegalStateException("boom"));
		});

		StepVerifier.withVirtualTime(() -> new MonoDelayElement<>(source,
				2,
				TimeUnit.SECONDS,
				defaultSchedulerForDelay()))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext("foo")
		            .verifyComplete();
		assertThat(dropped.get()).hasMessage("boom")
		                         .isInstanceOf(IllegalStateException.class);
	}

	@Test
	public void upstreamIsDelayedSource() {
		AtomicReference<Object> upstream = new AtomicReference<>();

		StepVerifier.withVirtualTime(() -> Mono.just(1).delayElement(Duration.ofSeconds
						(2),
				defaultSchedulerForDelay())
				.doOnSubscribe(s -> {
					assertThat(s).isInstanceOf(MonoDelayElement.DelayElementSubscriber.class);

					@SuppressWarnings("unchecked")
					MonoDelayElement.DelayElementSubscriber<Integer> delayedSubscriber =
							(MonoDelayElement.DelayElementSubscriber<Integer>) s;

					upstream.set(delayedSubscriber.scan(Scannable.Attr.PARENT));
				}))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void completeOnNextWithoutCancel() {
		AtomicInteger onCancel = new AtomicInteger();
		AtomicInteger sourceOnCancel = new AtomicInteger();
		AtomicInteger onTerminate = new AtomicInteger();
		AtomicInteger sourceOnTerminate = new AtomicInteger();
		Mono<String> source = Mono.<String>fromDirect(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext("foo");
		})
		.doOnCancel(sourceOnCancel::incrementAndGet)
		.doOnTerminate(sourceOnTerminate::incrementAndGet);


		StepVerifier.withVirtualTime(() -> new MonoDelayElement<>(source,
				2,
				TimeUnit.SECONDS,
				defaultSchedulerForDelay())
				.doOnCancel(onCancel::incrementAndGet)
				.doOnTerminate(onTerminate::incrementAndGet))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext("foo")
		            .verifyComplete();

		assertThat(onTerminate).hasValue(1);
		assertThat(sourceOnTerminate).hasValue(1);
		assertThat(onCancel).hasValue(0);
		assertThat(sourceOnCancel).hasValue(0);
	}

	@Test
	public void scanOperator() {
		MonoDelayElement<String> test = new MonoDelayElement<>(Mono.empty(), 1, TimeUnit.SECONDS, Schedulers.immediate());

		assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.immediate());
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoDelayElement.DelayElementSubscriber<String> test = new MonoDelayElement.DelayElementSubscriber<>(
				actual, Schedulers.single(), 10, TimeUnit.MILLISECONDS);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.single());
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}
}
