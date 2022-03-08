/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.Logger;
import reactor.util.Loggers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class MonoIgnoreThenTest {

	private static final Logger LOGGER = Loggers.getLogger(MonoIgnoreThenTest.class);

	@Test
	void multipleThenChainedCancelInTheMiddle() throws InterruptedException {
		List<Integer> startedSteps = new CopyOnWriteArrayList<>();
		List<Integer> completedSteps = new CopyOnWriteArrayList<>();
		List<Integer> cancelledSteps = new CopyOnWriteArrayList<>();

		Mono<?> run1 = Mono.fromRunnable(() -> multipleThenStep(1, startedSteps, completedSteps, cancelledSteps)).subscribeOn(Schedulers.boundedElastic());
		Mono<?> run2 = Mono.fromRunnable(() -> multipleThenStep(2, startedSteps, completedSteps, cancelledSteps)).subscribeOn(Schedulers.boundedElastic());
		Mono<?> run3 = Mono.fromRunnable(() -> multipleThenStep(3, startedSteps, completedSteps, cancelledSteps)).subscribeOn(Schedulers.boundedElastic());

		assertThat(Arrays.asList(run1, run2, run3)).as("smoke test: no callables").noneMatch(m -> m instanceof Callable);

		assertThatExceptionOfType(RuntimeException.class).isThrownBy(() ->
				run1.then(run2)
					.then(run3)
					.timeout(Duration.ofMillis(750))
					.block())
			.withCauseExactlyInstanceOf(TimeoutException.class);

		//we give 1.5s total in case run3 incorrectly starts or run2 incorrectly completes
		Thread.sleep(750);
		//we verify that run3 didn't even start
		assertThat(startedSteps).as("started steps").containsExactly(1, 2);
		//in this configuration the sleep is interrupted so run2 "done" should not happen
		assertThat(completedSteps).as("done steps").containsExactly(1);
		assertThat(cancelledSteps).as("interruptions").containsExactly(2);
	}

	@Test
	void multipleThenChainedCancelInTheMiddle_withCallables() throws InterruptedException {
		List<Integer> startedSteps = new CopyOnWriteArrayList<>();
		List<Integer> completedSteps = new CopyOnWriteArrayList<>();
		List<Integer> cancelledSteps = new CopyOnWriteArrayList<>();

		Mono<?> run1 = Mono.fromRunnable(() -> multipleThenStep(1, startedSteps, completedSteps, cancelledSteps));
		Mono<?> run2 = Mono.fromRunnable(() -> multipleThenStep(2, startedSteps, completedSteps, cancelledSteps));
		Mono<?> run3 = Mono.fromRunnable(() -> multipleThenStep(3, startedSteps, completedSteps, cancelledSteps));

		assertThat(Arrays.asList(run1, run2, run3)).as("smoke test: all callables").allMatch(m -> m instanceof Callable);

		assertThatExceptionOfType(RuntimeException.class).isThrownBy(() ->
				run1.then(run2)
					.then(run3)
				.timeout(Duration.ofMillis(750))
				.block())
				.withCauseExactlyInstanceOf(TimeoutException.class);

		//we give 1.5s total in case run3 incorrectly starts
		Thread.sleep(750);
		//we verify that run3 didn't even start
		assertThat(startedSteps).as("started steps").containsExactly(1, 2);
		//unfortunately in this configuration the sleep cannot really be interrupted so the "done" will still happen
		assertThat(completedSteps).as("done steps").containsExactly(1, 2);
		assertThat(cancelledSteps).as("no interruptions").isEmpty();
	}

	private void multipleThenStep(int i, Collection<Integer> startedSteps, Collection<Integer> completedSteps, Collection<Integer> cancelledSteps) {
		startedSteps.add(i);
		try {
			LOGGER.debug("Running stage {}", i);
			//this sleep is problematic usually, but done here on purpose in order to have a Callable Mono
			// that induces a sufficient delay for timeout to trigger
			Thread.sleep(500);
			LOGGER.debug("Running stage {} done", i);
			completedSteps.add(i);
		} catch (InterruptedException e) {
			cancelledSteps.add(i);
			LOGGER.debug("Stage {} has been interrupted / cancelled", i);
		}
	}


	@Nested
	class ThenIgnoreVariant {

		@Test
		void justThenIgnore() {
			StepVerifier.create(Mono.just(1)
									.then())
						.verifyComplete();
		}

		@Test
		void justThenEmptyThenIgnoreWithTime() {
			StepVerifier.withVirtualTime(() -> Mono.just(1).thenEmpty(Mono.delay(Duration.ofSeconds(123)).then()))
						.thenAwait(Duration.ofSeconds(123))
						.verifyComplete();
		}

		@Test
		void justThenIgnoreWithCancel() {
			TestPublisher<String> cancelTester = TestPublisher.create();

			Disposable disposable = cancelTester
					.flux()
					.then()
					.subscribe();

			disposable.dispose();

			cancelTester.assertCancelled();
		}
	}

	@Test
	@Tag("slow")
	// https://github.com/reactor/reactor-core/issues/2561
	void raceTest2561() {
		final Scheduler scheduler = Schedulers.newSingle("non-test-thread");
		final Mono<String> getCurrentThreadName =
				Mono.fromSupplier(() -> Thread.currentThread().getName());
		for (int i = 0; i < 100000; i++) {
			StepVerifier.create(getCurrentThreadName.publishOn(scheduler)
													.then(getCurrentThreadName)
													.then(getCurrentThreadName)
													.then(getCurrentThreadName))
						.assertNext(threadName -> {
							assertThat(threadName).startsWith("non-test-thread");
						})
						.verifyComplete();
		}
	}

	@Test
	void justThenEmpty() {
		StepVerifier.create(Mono.just(1)
								.thenEmpty(Flux.empty()))
					.verifyComplete();
	}

	@Test
	void justThenThen() {
		StepVerifier.create(Mono.just(0)
								.then(Mono.just(1))
								.then(Mono.just(2)))
					.expectNext(2)
					.verifyComplete();
	}

	@Test
	void justThenReturn() {
		StepVerifier.create(Mono.just(0).thenReturn(2))
		            .expectNext(2)
		            .verifyComplete();
	}

	//see https://github.com/reactor/reactor-core/issues/661
	@Test
	void fluxThenMonoAndShift() {
		StepVerifier.create(Flux.just("Good Morning", "Hello")
								.then(Mono.just("Good Afternoon"))
								.then(Mono.just("Bye")))
					.expectNext("Bye")
					.verifyComplete();
	}

	//see https://github.com/reactor/reactor-core/issues/661
	@Test
	void monoThenMonoAndShift() {
		StepVerifier.create(Mono.just("Good Morning")
								.then(Mono.just("Good Afternoon"))
								.then(Mono.just("Bye")))
					.expectNext("Bye")
					.verifyComplete();
	}

	@Test
	void scanOperator() {
		MonoIgnoreThen<String> test = new MonoIgnoreThen<>(new Publisher[]{Mono.just("foo")}, Mono.just("bar"));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	void scanThenIgnoreMain() {
		AssertSubscriber<String> actual = new AssertSubscriber<>();
		MonoIgnoreThen.ThenIgnoreMain<String> test = new MonoIgnoreThen.ThenIgnoreMain<>(actual, new Publisher[]{Mono.just("foo")}, Mono.just("bar"));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
