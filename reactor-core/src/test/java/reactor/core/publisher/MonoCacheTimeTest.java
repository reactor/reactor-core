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

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.test.RaceTestUtils;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.test.publisher.MonoOperatorTest;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoCacheTimeTest extends MonoOperatorTest<String, String> {

	@Test
	public void cacheDependingOnSignal() throws InterruptedException {
		AtomicInteger count = new AtomicInteger();

		Mono<Integer> source = Mono.fromCallable(count::incrementAndGet);

		Mono<Integer> cached = new MonoCacheTime<>(source,
				sig -> {
					if (sig.isOnNext()) {
						return Duration.ofMillis(100 * sig.get());
					}
					return Duration.ZERO;
				}, Schedulers.parallel());

		cached.block();
		cached.block();
		assertThat(cached.block())
				.as("after 110ms")
				.isEqualTo(1);

		Thread.sleep(110);
		cached.block();
		assertThat(cached.block())
				.as("after 220ms")
				.isEqualTo(2);

		Thread.sleep(110);
		assertThat(cached.block())
				.as("after 330ms")
				.isEqualTo(2);

		Thread.sleep(110);
		cached.block();
		assertThat(cached.block())
				.as("after 440ms")
				.isEqualTo(3);

		assertThat(count).hasValue(3);
	}

	@Test
	public void cacheDependingOnValueAndError() throws InterruptedException {
		AtomicInteger count = new AtomicInteger();

		Mono<Integer> source = Mono.fromCallable(count::incrementAndGet)
				.map(i -> {
					if (i == 2) throw new IllegalStateException("transient boom");
					return i;
				});

		AtomicInteger onNextTtl = new AtomicInteger();
		AtomicInteger onErrorTtl = new AtomicInteger();
		AtomicInteger onEmptyTtl = new AtomicInteger();

		Mono<Integer> cached = new MonoCacheTime<>(source,
				v -> { onNextTtl.incrementAndGet(); return Duration.ofMillis(100 * v);},
				e -> { onErrorTtl.incrementAndGet(); return Duration.ofMillis(300);},
				() -> { onEmptyTtl.incrementAndGet(); return Duration.ZERO;} ,
				Schedulers.parallel());

		cached.block();
		cached.block();
		assertThat(cached.block())
				.as("1")
				.isEqualTo(1);

		Thread.sleep(110);
		assertThatExceptionOfType(IllegalStateException.class)
				.as("2 errors")
				.isThrownBy(cached::block);

		Thread.sleep(210);
		assertThatExceptionOfType(IllegalStateException.class)
				.as("2 still errors")
				.isThrownBy(cached::block);
		assertThat(count).as("2 is cached").hasValue(2);

		Thread.sleep(110);
		assertThat(cached.block())
				.as("3 emits again")
				.isEqualTo(3);

		Thread.sleep(210);
		assertThat(cached.block())
				.as("3 is cached")
				.isEqualTo(3);

		assertThat(count).hasValue(3);
		assertThat(onNextTtl).as("onNext TTL generations").hasValue(2);
		assertThat(onErrorTtl).as("onError TTL generations").hasValue(1);
		assertThat(onEmptyTtl).as("onEmpty TTL generations").hasValue(0);
	}

	@Test
	public void cacheDependingOnEmpty() throws InterruptedException {
		AtomicInteger count = new AtomicInteger();

		Mono<Integer> source = Mono.fromCallable(count::incrementAndGet)
		                           .filter(i -> i > 1);

		AtomicInteger onNextTtl = new AtomicInteger();
		AtomicInteger onErrorTtl = new AtomicInteger();
		AtomicInteger onEmptyTtl = new AtomicInteger();

		Mono<Integer> cached = new MonoCacheTime<>(source,
				v -> { onNextTtl.incrementAndGet(); return Duration.ofMillis(100 * v);},
				e -> { onErrorTtl.incrementAndGet(); return Duration.ofMillis(4000);},
				() -> { onEmptyTtl.incrementAndGet(); return Duration.ofMillis(300);} ,
				Schedulers.parallel());

		assertThat(cached.block())
				.as("1 immediate")
				.isNull();

		Thread.sleep(110);
		assertThat(cached.block())
				.as("1 after 100ms")
				.isNull();

		Thread.sleep(110);
		assertThat(cached.block())
				.as("1 after 200ms")
				.isNull();

		Thread.sleep(110);
		assertThat(cached.block())
				.as("2")
				.isEqualTo(2);

		Thread.sleep(110);
		assertThat(cached.block())
				.as("2 after 100ms")
				.isEqualTo(2);

		Thread.sleep(110);
		assertThat(cached.block())
				.as("3 after 200ms")
				.isEqualTo(3);

		assertThat(count).hasValue(3);
		assertThat(onNextTtl).as("onNext TTL generations").hasValue(2);
		assertThat(onErrorTtl).as("onError TTL generations").hasValue(0);
		assertThat(onEmptyTtl).as("onEmpty TTL generations").hasValue(1);
	}

	@Test
	public void nextTtlGeneratorFailure() {
		Mono<Integer> cached = new MonoCacheTime<>(Mono.just(0),
				v -> Duration.ofMillis(400 / v),
				t -> Duration.ZERO,
				() -> Duration.ZERO,
				Schedulers.parallel());

		StepVerifier.create(cached)
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(ArithmeticException.class)
				            .hasNoSuppressedExceptions())
		            .verifyThenAssertThat()
		            .hasDropped(0)
		            .hasNotDroppedErrors();
	}

	@Test
	public void emptyTtlGeneratorFailure() {
		Mono<Integer> cached = new MonoCacheTime<>(Mono.empty(),
				Duration::ofSeconds,
				t -> Duration.ofSeconds(10),
				() -> { throw new IllegalStateException("boom"); },
				Schedulers.parallel());

		StepVerifier.create(cached)
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("boom")
				            .hasNoSuppressedExceptions())
		            .verifyThenAssertThat()
		            .hasNotDroppedElements()
		            .hasNotDroppedErrors();
	}

	@Test
	public void errorTtlGeneratorFailure() {
		Throwable exception = new IllegalArgumentException("foo");
		Mono<Integer> cached = new MonoCacheTime<>(Mono.error(exception),
				Duration::ofSeconds,
				t -> { throw new IllegalStateException("boom"); },
				() -> Duration.ZERO,
				Schedulers.parallel());

		StepVerifier.create(cached)
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("boom")
				            .hasSuppressedException(exception))
		            .verifyThenAssertThat()
		            .hasNotDroppedElements()
		            .hasDroppedErrorWithMessage("foo");
	}

	@Test
	public void nextTtlGeneratorTransientFailure() {
		AtomicInteger count = new AtomicInteger();

		Mono<Integer> cached = new MonoCacheTime<>(Mono.fromCallable(count::incrementAndGet),
				v -> {
					if (v == 1) throw new IllegalStateException("transient");
					return Duration.ofMillis(100 * v);
				},
				t -> Duration.ZERO,
				() -> Duration.ZERO,
				Schedulers.parallel());

		StepVerifier.create(cached)
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("transient")
				            .hasNoSuppressedExceptions())
		            .verifyThenAssertThat()
		            .hasDropped(1)
		            .hasNotDroppedErrors();

		StepVerifier.create(cached)
		            .expectNext(2)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasNotDroppedErrors()
		            .hasNotDroppedElements();

		assertThat(cached.block())
				.as("cached after cache miss")
				.isEqualTo(2);

		assertThat(count).as("source invocations")
		                 .hasValue(2);
	}

	@Test
	public void emptyTtlGeneratorTransientFailure() {
		AtomicInteger count = new AtomicInteger();

		Mono<Integer> cached = new MonoCacheTime<>(Mono.empty(),
				v -> Duration.ofSeconds(10),
				t -> Duration.ofSeconds(10),
				() -> {
					if (count.incrementAndGet() == 1) throw new IllegalStateException("transient");
					return Duration.ofMillis(100);
				},
				Schedulers.parallel());

		StepVerifier.create(cached)
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("transient")
				            .hasNoSuppressedExceptions())
		            .verifyThenAssertThat()
		            .hasNotDroppedElements()
		            .hasNotDroppedErrors();

		StepVerifier.create(cached)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasNotDroppedErrors()
		            .hasNotDroppedElements();

		assertThat(cached.block())
				.as("cached after cache miss")
				.isNull();

		assertThat(count).as("source invocations")
		                 .hasValue(2);
	}

	@Test
	public void errorTtlGeneratorTransientFailure() {
		Throwable exception = new IllegalArgumentException("foo");
		AtomicInteger count = new AtomicInteger();

		Mono<Integer> cached = new MonoCacheTime<>(Mono.error(exception),
				v -> Duration.ofSeconds(10),
				t -> {
					if (count.incrementAndGet() == 1) throw new IllegalStateException("transient");
					return Duration.ofMillis(100);
				},
				() -> Duration.ZERO,
				Schedulers.parallel());

		StepVerifier.create(cached)
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("transient")
				            .hasSuppressedException(exception))
		            .verifyThenAssertThat()
		            .hasNotDroppedElements()
		            .hasDroppedErrorWithMessage("foo");

		StepVerifier.create(cached)
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalArgumentException.class)
				            .hasMessage("foo"))
		            .verifyThenAssertThat()
		            .hasNotDroppedErrors()
		            .hasNotDroppedElements();

		assertThatExceptionOfType(IllegalArgumentException.class)
				.as("cached after cache miss")
				.isThrownBy(cached::block);

		assertThat(count).as("source invocations")
		                 .hasValue(2);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Collections.singletonList(scenario(f -> f.cache(Duration.ofMillis(100))));
	}

	@Test
	public void expireAfterTtlNormal() throws InterruptedException {
		AtomicInteger subCount = new AtomicInteger();
		Mono<Integer> source = Mono.defer(() -> Mono.just(subCount.incrementAndGet()));

		Mono<Integer> cached = source.cache(Duration.ofMillis(100))
		                             .hide();

		StepVerifier.create(cached)
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .as("first subscription caches 1")
		            .verifyComplete();

		Thread.sleep(110);

		StepVerifier.create(cached)
		            .expectNext(2)
		            .as("cached value should expire")
		            .verifyComplete();

		assertThat(subCount.get()).isEqualTo(2);
	}

	@Test
	public void doesntResubscribeNormal() {
		AtomicInteger subCount = new AtomicInteger();
		Mono<Integer> source = Mono.defer(() -> Mono.just(subCount.incrementAndGet()));

		Mono<Integer> cached = source.cache(Duration.ofMillis(100))
		                             .hide();

		StepVerifier.create(cached)
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .as("first subscription caches 1")
		            .verifyComplete();

		StepVerifier.create(cached)
		            .expectNext(1)
		            .as("second subscription uses cache")
		            .verifyComplete();

		assertThat(subCount.get()).isEqualTo(1);
	}


	@Test
	public void expireAfterTtlConditional() throws InterruptedException {
		AtomicInteger subCount = new AtomicInteger();
		Mono<Integer> source = Mono.defer(() -> Mono.just(subCount.incrementAndGet()));

		Mono<Integer> cached = source.cache(Duration.ofMillis(100))
				.hide()
				.filter(always -> true);

		StepVerifier.create(cached)
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .as("first subscription caches 1")
		            .verifyComplete();

		Thread.sleep(110);

		StepVerifier.create(cached)
		            .expectNext(2)
		            .as("cached value should expire")
		            .verifyComplete();

		assertThat(subCount.get()).isEqualTo(2);
	}

	@Test
	public void doesntResubscribeConditional() {
		AtomicInteger subCount = new AtomicInteger();
		Mono<Integer> source = Mono.defer(() -> Mono.just(subCount.incrementAndGet()));

		Mono<Integer> cached = source.cache(Duration.ofMillis(100))
				.hide()
				.filter(always -> true);

		StepVerifier.create(cached)
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .as("first subscription caches 1")
		            .verifyComplete();

		StepVerifier.create(cached)
		            .expectNext(1)
		            .as("second subscription uses cache")
		            .verifyComplete();

		assertThat(subCount.get()).isEqualTo(1);
	}

	@Test
	public void totalCancelDoesntCancelSource() {
		AtomicInteger cancelled = new AtomicInteger();
		Mono<Object> cached = Mono.never()
		                          .doOnCancel(cancelled::incrementAndGet)
		                          .cache(Duration.ofMillis(200));

		Disposable d1 = cached.subscribe();
		Disposable d2 = cached.subscribe();

		d1.dispose();
		d2.dispose();

		assertThat(cancelled.get()).isEqualTo(0);
	}

	@Test
	public void totalCancelCanResubscribe() {
		AtomicInteger cancelled = new AtomicInteger();
		AtomicInteger subscribed = new AtomicInteger();
		TestPublisher<Integer> source = TestPublisher.create();
		Mono<Integer> cached = source.mono()
		                             .doOnSubscribe(s -> subscribed.incrementAndGet())
		                            .doOnCancel(cancelled::incrementAndGet)
		                            .cache(Duration.ofMillis(200));

		Disposable d1 = cached.subscribe();
		Disposable d2 = cached.subscribe();

		d1.dispose();
		d2.dispose();

		assertThat(cancelled.get()).isEqualTo(0);
		assertThat(subscribed.get()).isEqualTo(1);

		StepVerifier.create(cached)
		            .then(() -> source.emit(100))
		            .expectNext(100)
		            .verifyComplete();

		assertThat(cancelled.get()).isEqualTo(0);
		assertThat(subscribed.get()).isEqualTo(1);
	}

	@Test
	public void partialCancelDoesntCancelSource() {
		AtomicInteger cancelled = new AtomicInteger();
		Mono<Object> cached = Mono.never()
		                          .doOnCancel(cancelled::incrementAndGet)
		                          .cache(Duration.ofMillis(200));

		Disposable d1 = cached.subscribe();
		Disposable d2 = cached.subscribe();

		d1.dispose();

		assertThat(cancelled.get()).isEqualTo(0);
	}

	@Test
	public void raceSubscribeAndCache() {
		AtomicInteger count = new AtomicInteger();
		Mono<Integer> source = Mono.fromCallable(count::getAndIncrement);

		for (int i = 0; i < 500; i++) {
			Mono<Integer> cached;
			if (i == 0) {
				cached = source.log().cache(Duration.ofSeconds(2));
			}
			else {
				cached = source.cache(Duration.ofSeconds(2));
			}
			RaceTestUtils.race(cached::subscribe, cached::subscribe);
		}

		assertThat(count.get()).isEqualTo(500);
	}

	@Test
	public void sourceCachedNoCoordinatorLeak() {
		TestPublisher<Integer> source = TestPublisher.create();
		MonoCacheTime<Integer> cached = new MonoCacheTime<>(source.mono(), Duration.ofSeconds(2),
				Schedulers.parallel());
		cached.subscribe();
		WeakReference<Signal<Integer>> refCoordinator = new WeakReference<>(cached.state);

		assertThat(refCoordinator.get()).isInstanceOf(MonoCacheTime.CoordinatorSubscriber.class);

		source.emit(100);
		System.gc();

		assertThat(refCoordinator.get()).isNull();
	}

	@Test
	public void coordinatorReachableThroughCacheInnerSubscriptionsOnly() throws InterruptedException {
		TestPublisher<Integer> source = TestPublisher.create();

		MonoCacheTime<Integer> cached = new MonoCacheTime<>(source.mono(),
				Duration.ofMillis(100), //short cache TTL should trigger state change if source is not never
				Schedulers.parallel());

		Disposable d1 = cached.subscribe();
		cached.subscribe();

		WeakReference<Signal<Integer>> refCoordinator = new WeakReference<>(cached.state);

		assertThat(refCoordinator.get()).isInstanceOf(MonoCacheTime.CoordinatorSubscriber.class);

		Thread.sleep(150);
		source = null;
		cached = null;
		System.gc();

		assertThat(refCoordinator.get()).isInstanceOf(MonoCacheTime.CoordinatorSubscriber.class);
	}

	@Test
	public void coordinatorCacheInnerDisposedOrNoReferenceNoLeak() throws InterruptedException {
		TestPublisher<Integer> source = TestPublisher.create();

		MonoCacheTime<Integer> cached = new MonoCacheTime<>(source.mono(),
				Duration.ofMillis(100), //short cache TTL should trigger state change if source is not never
				Schedulers.parallel());

		Disposable d1 = cached.subscribe();
		cached.subscribe();

		WeakReference<Signal<Integer>> refCoordinator = new WeakReference<>(cached.state);

		assertThat(refCoordinator.get()).isInstanceOf(MonoCacheTime.CoordinatorSubscriber.class);

		Thread.sleep(150);
		source = null;
		cached = null;
		d1.dispose();
		System.gc();

		assertThat(refCoordinator.get()).isNull();
	}

	@Test
	public void coordinatorNoReferenceNoLeak() throws InterruptedException {
		TestPublisher<Integer> source = TestPublisher.create();

		MonoCacheTime<Integer> cached = new MonoCacheTime<>(source.mono(),
				Duration.ofMillis(100), //short cache TTL should trigger state change if source is not never
				Schedulers.parallel());

		cached.subscribe();
		cached.subscribe();

		WeakReference<Signal<Integer>> refCoordinator = new WeakReference<>(cached.state);

		assertThat(refCoordinator.get()).isInstanceOf(MonoCacheTime.CoordinatorSubscriber.class);

		Thread.sleep(150);
		source = null;
		cached = null;
		System.gc();

		assertThat(refCoordinator.get()).isNull();
	}

}