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

import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.MonoOperatorTest;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoCacheTimeTest extends MonoOperatorTest<String, String> {

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