/*
 * Copyright (c) 2025 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.util.repeat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

class RepeatSpecTest {

	@Test
	public void builderMethodsProduceNewInstances() {
		RepeatSpec init = RepeatSpec.times(1);

		assertThat(init).isNotSameAs(init.onlyIf(signal -> true))
		                .isNotSameAs(init.doBeforeRepeat(signal -> {
		                }))
		                .isNotSameAs(init.doAfterRepeat(signal -> {
		                }))
		                .isNotSameAs(init.withFixedDelay(Duration.ofMillis(100)))
		                .isNotSameAs(init.jitter(0.5))
		                .isNotSameAs(init.withScheduler(Schedulers.immediate()));
	}

	@Test
	void onlyIfReplacesPredicate() {
		RepeatSpec repeatSpec = RepeatSpec.times(5)
		                                  .onlyIf(signal -> signal.iteration() < 5)
		                                  .onlyIf(signal -> signal.iteration() == 0);

		Repeat.RepeatSignal acceptSignal =
				new ImmutableRepeatSignal(0, 123L, Duration.ofMillis(0), Context.empty());

		Repeat.RepeatSignal rejectSignal =
				new ImmutableRepeatSignal(4, 123L, Duration.ofMillis(0), Context.empty());

		assertThat(repeatSpec.repeatPredicate).accepts(acceptSignal)
		                                      .rejects(rejectSignal);
	}

	@Test
	void doBeforeRepeatIsCumulative() {
		AtomicInteger counter = new AtomicInteger();

		Repeat.RepeatSignal dummySignal =
				new ImmutableRepeatSignal(0, 0L, Duration.ZERO, Context.empty());

		RepeatSpec repeatSpec = RepeatSpec.times(1)
		                                  .doBeforeRepeat(signal -> counter.incrementAndGet())
		                                  .doBeforeRepeat(signal -> counter.addAndGet(10));

		repeatSpec.beforeRepeatHook.accept(dummySignal);

		assertThat(counter.get()).isEqualTo(11);
	}

	@Test
	void doAfterRepeatIsCumulative() {
		AtomicInteger counter = new AtomicInteger();

		Repeat.RepeatSignal dummySignal =
				new ImmutableRepeatSignal(0, 0L, Duration.ZERO, Context.empty());

		RepeatSpec repeatSpec = RepeatSpec.times(1)
		                                  .doAfterRepeat(signal -> counter.incrementAndGet())
		                                  .doAfterRepeat(signal -> counter.addAndGet(10));

		repeatSpec.afterRepeatHook.accept(dummySignal);

		assertThat(counter.get()).isEqualTo(11);
	}

	@Test
	void settersApplyConfigurationCorrectly() {
		Duration delay = Duration.ofMillis(123);
		double jitter = 0.42;
		Scheduler single = Schedulers.single();

		RepeatSpec repeatSpec = RepeatSpec.times(3)
		                                  .withFixedDelay(delay)
		                                  .jitter(jitter)
		                                  .withScheduler(single);

		assertThat(repeatSpec.fixedDelay).isEqualTo(delay);
		assertThat(repeatSpec.jitterFactor).isEqualTo(jitter);
		assertThat(repeatSpec.scheduler).isSameAs(single);
	}

	@Test
	void repeatMaxLimitsRepeats() {
		AtomicInteger subscriptionCounter = new AtomicInteger();

		Flux<String> flux = Flux.defer(() -> {
			subscriptionCounter.incrementAndGet();
			return Flux.just("foo", "bar");
		});

		Flux<String> repeated = flux.repeatWhen(RepeatSpec.times(3));

		StepVerifier.create(repeated)
		            .expectNext("foo", "bar", "foo", "bar", "foo", "bar", "foo", "bar")
		            .expectComplete()
		            .verify();

		assertThat(subscriptionCounter.get()).isEqualTo(4);
	}

	@Test
	void onlyIfPredicateControlsTermination() {
		AtomicInteger attempts = new AtomicInteger();

		Flux<String> source = Flux.defer(() -> {
			int i = attempts.getAndIncrement();
			return Flux.just("foo" + i);
		});

		RepeatSpec repeatSpec = RepeatSpec.times(100)
		                                  .onlyIf(signal -> signal.iteration() < 2);

		StepVerifier.create(source.repeatWhen(repeatSpec))
		            .expectNext("foo0", "foo1", "foo2")
		            .expectComplete()
		            .verify();

		assertThat(attempts.get()).isEqualTo(3);
	}

	@Test
	void jitterProducesDelayInExpectedRange() {
		Duration baseDelay = Duration.ofMillis(1000);
		double jitterFactor = 0.2;
		int samples = 1000;

		RepeatSpec repeatSpec = RepeatSpec.times(1)
		                                  .withFixedDelay(baseDelay)
		                                  .jitter(jitterFactor);

		long minExpected = (long) (baseDelay.toMillis() * (1 - jitterFactor));
		long maxExpected = (long) (baseDelay.toMillis() * (1 + jitterFactor));

		for (int i = 0; i < samples; i++) {
			Duration actual = repeatSpec.calculateDelay();
			long millis = actual.toMillis();
			assertThat(millis).as("Delay in range")
			                  .isBetween(minExpected, maxExpected);
		}
	}

	@Test
	void repeatContextCanInfluencePredicate() {
		RepeatSpec repeatSpec = RepeatSpec.times(5)
		                                  .withRepeatContext(Context.of("stopAfterOne",
				                                  true))
		                                  .onlyIf(signal -> {
			                                  boolean stopAfterOne =
					                                  signal.repeatContextView()
					                                        .getOrDefault("stopAfterOne",
							                                        false);
			                                  return !stopAfterOne || signal.iteration() == 0;
		                                  });

		AtomicInteger subscriptionCount = new AtomicInteger();
		Flux<String> source = Flux.defer(() -> {
			subscriptionCount.incrementAndGet();
			return Flux.just("foo");
		});

		StepVerifier.create(source.repeatWhen(repeatSpec))
		            .expectNext("foo", "foo")
		            .verifyComplete();

		assertThat(subscriptionCount.get()).isEqualTo(2);
	}
}