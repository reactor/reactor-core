/*
 * Copyright (c) 2015-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoDelayTest {

	Mono<Long> scenario_delayedSource() {
		return Mono.delay(Duration.ofSeconds(4));
	}

	@Test
	public void delayedSource() {
		StepVerifier.withVirtualTime(this::scenario_delayedSource)
		            .thenAwait(Duration.ofSeconds(4))
		            .expectNext(0L)
		            .verifyComplete();
	}

	Mono<Long> scenario_delayedSourceError() {
		return Mono.delay(Duration.ofSeconds(4));
	}

	@Test
	public void delayedSourceError() {
		StepVerifier.withVirtualTime(this::scenario_delayedSourceError, StepVerifierOptions.create()
				.initialRequest(0)
				.virtualTimeSchedulerSupplier(VirtualTimeScheduler::create)
				.withInitialContext(Context.of(MonoDelay.CONTEXT_OPT_OUT_NOBACKPRESSURE, true))
		)
				.thenAwait(Duration.ofSeconds(5))
				.verifyErrorMatches(Exceptions::isOverflow);
	}

	@Test
	public void multipleDelaysUsingDefaultScheduler() throws InterruptedException {
		AtomicLong counter = new AtomicLong();

		Mono.delay(Duration.ofMillis(50)).subscribe(v -> counter.incrementAndGet());
		Mono.delay(Duration.ofMillis(100)).subscribe(v -> counter.incrementAndGet());
		Mono.delay(Duration.ofMillis(150)).subscribe(v -> counter.incrementAndGet());
		Mono.delay(Duration.ofMillis(200)).subscribe(v -> counter.incrementAndGet());

		assertThat(counter.intValue()).isEqualTo(0);

		Thread.sleep(110);
		assertThat(counter.intValue()).isEqualTo(2);

		Thread.sleep(110);
		assertThat(counter.intValue()).isEqualTo(4);
	}

	@Test
	public void scanOperator() {
		MonoDelay test = new MonoDelay(1, TimeUnit.SECONDS, Schedulers.immediate());

		assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.immediate());
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}

	@Test
	public void scanDelayRunnable() {
		CoreSubscriber<Long> actual = new LambdaMonoSubscriber<>(null, e -> {
		}, null, null);
		MonoDelay.MonoDelayRunnable test = new MonoDelay.MonoDelayRunnable(actual, true);

		actual.onSubscribe(test);

		assertThat(test.scan(Scannable.Attr.PARENT)).isNull();
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.request(1);
		test.run();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
	}

	@Test
	public void scanDelayRunnableCancelled() {
		CoreSubscriber<Long> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoDelay.MonoDelayRunnable test = new MonoDelay.MonoDelayRunnable(actual, true);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	void simulateDelayShorterThanOnSubscribeToRequestInterval() {
		StepVerifier.withVirtualTime(() -> Mono.delay(Duration.ofNanos(1)), 0)
				.expectSubscription()
				.expectNoEvent(Duration.ofMillis(1))
				.thenRequest(1)
				.expectNext(0L)
				.expectComplete()
				.verify(Duration.ofMillis(100));
	}

	@Test
	void simulateDelayShorterThanOnSubscribeToRequestInterval_optInBackpressure() {
		StepVerifierOptions options = StepVerifierOptions
				.create()
				.initialRequest(0L)
				.virtualTimeSchedulerSupplier(VirtualTimeScheduler::create)
				// MonoDelay.CONTEXT_OPT_OUT_NOBACKPRESSURE: true
				.withInitialContext(Context.of("reactor.core.publisher.MonoDelay.failOnBackpressure", true));

		StepVerifier.withVirtualTime(() -> Mono.delay(Duration.ofNanos(1)), options)
				.expectSubscription()
				.expectNoEvent(Duration.ofNanos(1))
				.expectErrorMatches(Exceptions::isOverflow)
				.verify(Duration.ofMillis(100));
	}
}
