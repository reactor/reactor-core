/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.retry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;


import org.junit.jupiter.api.Test;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxRetryWhenTest;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public class RetryBackoffSpecTest {

	@Test
	public void builderMethodsProduceNewInstances() {
		RetryBackoffSpec init = Retry.backoff(1, Duration.ZERO);
		assertThat(init)
				.isNotSameAs(init.minBackoff(Duration.ofSeconds(1)))
				.isNotSameAs(init.maxBackoff(Duration.ZERO))
				.isNotSameAs(init.jitter(0.5d))
				.isNotSameAs(init.scheduler(Schedulers.parallel()))
				.isNotSameAs(init.maxAttempts(10))
				.isNotSameAs(init.filter(t -> true))
				.isNotSameAs(init.modifyErrorFilter(predicate -> predicate.and(t -> true)))
				.isNotSameAs(init.transientErrors(true))
				.isNotSameAs(init.doBeforeRetry(rs -> {}))
				.isNotSameAs(init.doAfterRetry(rs -> {}))
				.isNotSameAs(init.doBeforeRetryAsync(rs -> Mono.empty()))
				.isNotSameAs(init.doAfterRetryAsync(rs -> Mono.empty()))
				.isNotSameAs(init.onRetryExhaustedThrow((b, rs) -> new IllegalStateException("boon")))
				.isNotSameAs(init.withRetryContext(Context.of("foo", "bar")));
	}

	@Test
	public void retryContextIsCorrectlyPropagatedAndSet() {
		RetryBackoffSpec init = Retry.backoff(1L, Duration.ZERO);
		assertThat(init.withRetryContext(Context.of("foo", "bar")).maxAttempts(10))
				.satisfies(rs -> rs.retryContext().get("foo").equals("bar"));
	}


	@Test
	public void builderCanBeUsedAsTemplate() {
		//a base builder can be reused across several Flux with different tuning for each flux
		RetryBackoffSpec template = Retry.backoff(1, Duration.ZERO).transientErrors(false);

		Supplier<Flux<Integer>> transientError = () -> {
			AtomicInteger errorOnEven = new AtomicInteger();
			return Flux.generate(sink -> {
				int i = errorOnEven.getAndIncrement();
				if (i == 5) {
					sink.complete();
				}
				if (i % 2 == 0) {
					sink.error(new IllegalStateException("boom " + i));
				}
				else {
					sink.next(i);
				}
			});
		};

		Flux<Integer> modifiedTemplate1 = transientError.get().retryWhen(template.maxAttempts(2));
		Flux<Integer> modifiedTemplate2 = transientError.get().retryWhen(template.transientErrors(true));

		StepVerifier.create(modifiedTemplate1, StepVerifierOptions.create().scenarioName("modified template 1"))
		            .expectNext(1, 3)
		            .verifyErrorSatisfies(t -> assertThat(t)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("Retries exhausted: 2/2")
				            .hasCause(new IllegalStateException("boom 4")));

		StepVerifier.create(modifiedTemplate2, StepVerifierOptions.create().scenarioName("modified template 2"))
		            .expectNext(1, 3)
		            .verifyComplete();
	}

	@Test
	public void throwablePredicateReplacesThePredicate() {
		RetryBackoffSpec retryBuilder = Retry.backoff(1, Duration.ZERO)
		                                     .filter(t -> t instanceof RuntimeException)
		                                     .filter(t -> t instanceof IllegalStateException);

		assertThat(retryBuilder.errorFilter)
				.accepts(new IllegalStateException())
				.rejects(new IllegalArgumentException())
				.rejects(new RuntimeException());
	}

	@Test
	public void throwablePredicateModifierAugmentsThePredicate() {
		RetryBackoffSpec retryBuilder = Retry.backoff(1, Duration.ZERO)
		                                     .filter(t -> t instanceof RuntimeException)
		                                     .modifyErrorFilter(p -> p.and(t -> t.getMessage().length() == 3));

		assertThat(retryBuilder.errorFilter)
				.accepts(new IllegalStateException("foo"))
				.accepts(new IllegalArgumentException("bar"))
				.accepts(new RuntimeException("baz"))
				.rejects(new RuntimeException("too big"));
	}

	@Test
	public void throwablePredicateModifierWorksIfNoPreviousPredicate() {
		RetryBackoffSpec retryBuilder = Retry.backoff(1, Duration.ZERO)
		                                     .modifyErrorFilter(p -> p.and(t -> t.getMessage().length() == 3));

		assertThat(retryBuilder.errorFilter)
				.accepts(new IllegalStateException("foo"))
				.accepts(new IllegalArgumentException("bar"))
				.accepts(new RuntimeException("baz"))
				.rejects(new RuntimeException("too big"));
	}

	@Test
	public void throwablePredicateModifierRejectsNullGenerator() {
		assertThatNullPointerException().isThrownBy(() -> Retry.backoff(1, Duration.ZERO).modifyErrorFilter(p -> null))
		                                .withMessage("predicateAdjuster must return a new predicate");
	}

	@Test
	public void throwablePredicateModifierRejectsNullFunction() {
		assertThatNullPointerException().isThrownBy(() -> Retry.backoff(1, Duration.ZERO).modifyErrorFilter(null))
		                                .withMessage("predicateAdjuster");
	}

	@Test
	public void doBeforeRetryIsCumulative() {
		AtomicInteger atomic = new AtomicInteger();
		RetryBackoffSpec retryBuilder = Retry
				.backoff(1, Duration.ZERO)
				.doBeforeRetry(rs -> atomic.incrementAndGet())
				.doBeforeRetry(rs -> atomic.addAndGet(100));

		retryBuilder.syncPreRetry.accept(null);

		assertThat(atomic).hasValue(101);
	}

	@Test
	public void doAfterRetryIsCumulative() {
		AtomicInteger atomic = new AtomicInteger();
		RetryBackoffSpec retryBuilder = Retry
				.backoff(1, Duration.ZERO)
				.doAfterRetry(rs -> atomic.incrementAndGet())
				.doAfterRetry(rs -> atomic.addAndGet(100));

		retryBuilder.syncPostRetry.accept(null);

		assertThat(atomic).hasValue(101);
	}

	@Test
	public void delayRetryWithIsCumulative() {
		AtomicInteger atomic = new AtomicInteger();
		RetryBackoffSpec retryBuilder = Retry
				.backoff(1, Duration.ZERO)
				.doBeforeRetryAsync(rs -> Mono.fromRunnable(atomic::incrementAndGet))
				.doBeforeRetryAsync(rs -> Mono.fromRunnable(() -> atomic.addAndGet(100)));

		retryBuilder.asyncPreRetry.apply(null, Mono.empty()).block();

		assertThat(atomic).hasValue(101);
	}

	@Test
	public void retryThenIsCumulative() {
		AtomicInteger atomic = new AtomicInteger();
		RetryBackoffSpec retryBuilder = Retry
				.backoff(1, Duration.ZERO)
				.doAfterRetryAsync(rs -> Mono.fromRunnable(atomic::incrementAndGet))
				.doAfterRetryAsync(rs -> Mono.fromRunnable(() -> atomic.addAndGet(100)));

		retryBuilder.asyncPostRetry.apply(null, Mono.empty()).block();

		assertThat(atomic).hasValue(101);
	}

	@Test
	public void retryExceptionDefaultsToRetryExhausted() {
		RetryBackoffSpec retryBuilder = Retry.backoff(50, Duration.ZERO).transientErrors(true);

		final ImmutableRetrySignal trigger = new ImmutableRetrySignal(100, 50, new IllegalStateException("boom"));

		StepVerifier.create(retryBuilder.generateCompanion(Flux.just(trigger)))
		            .expectErrorSatisfies(e -> assertThat(e).matches(Exceptions::isRetryExhausted, "isRetryExhausted")
		                                                    .hasMessage("Retries exhausted: 50/50 in a row (100 total)")
		                                                    .hasCause(new IllegalStateException("boom")))
		            .verify();
	}

	@Test
	public void retryExceptionCanBeCustomized() {
		RetryBackoffSpec retryBuilder = Retry.backoff(1, Duration.ofMillis(123))
		                                     .onRetryExhaustedThrow((builder, rs) -> new IllegalArgumentException(builder.minBackoff.toString()));

		final ImmutableRetrySignal trigger = new ImmutableRetrySignal(100, 21, new IllegalStateException("boom"));

		StepVerifier.create(retryBuilder.generateCompanion(Flux.just(trigger)))
		            .expectErrorSatisfies(e -> assertThat(e).matches(t -> !Exceptions.isRetryExhausted(t), "is not retryExhausted")
		                                                    .hasMessage("PT0.123S")
		                                                    .hasNoCause())
		            .verify();
	}

	@Test
	public void defaultRetryExhaustedMessageWithNoTransientErrors() {
		assertThat(RetryBackoffSpec.BACKOFF_EXCEPTION_GENERATOR.apply(Retry.backoff(123, Duration.ZERO),
				new ImmutableRetrySignal(123, 123, null)))
				.hasMessage("Retries exhausted: 123/123");
	}

	@Test
	public void defaultRetryExhaustedMessageWithTransientErrors() {
		assertThat(RetryBackoffSpec.BACKOFF_EXCEPTION_GENERATOR.apply(Retry.backoff(12, Duration.ZERO).transientErrors(true),
				new ImmutableRetrySignal(123, 12, null)))
				.hasMessage("Retries exhausted: 12/12 in a row (123 total)");
	}

	@Test
	public void companionWaitsForAllHooksBeforeTrigger() {
		//this tests the companion directly, vs cumulatedRetryHooks which test full integration in the retryWhen operator
		IllegalArgumentException ignored = new IllegalArgumentException("ignored");
		Retry.RetrySignal sig1 = new ImmutableRetrySignal(1, 1, ignored);
		Retry.RetrySignal sig2 = new ImmutableRetrySignal(2, 1, ignored);
		Retry.RetrySignal sig3 = new ImmutableRetrySignal(3, 1, ignored);

		RetryBackoffSpec retryBuilder = Retry.backoff(10, Duration.ZERO).doAfterRetryAsync(rs -> Mono.delay(Duration.ofMillis(100 * (3 - rs.totalRetries()))).then());

		StepVerifier.create(retryBuilder.generateCompanion(Flux.just(sig1, sig2, sig3).hide()))
		            .expectNext(1L, 2L, 3L)
		            .verifyComplete();
	}

	@Test
	public void cumulatedRetryHooks() {
		List<String> order = new CopyOnWriteArrayList<>();
		AtomicInteger beforeHookTracker = new AtomicInteger();
		AtomicInteger afterHookTracker = new AtomicInteger();

		RetryBackoffSpec retryBuilder = Retry
				.backoff(1, Duration.ZERO)
				.doBeforeRetry(s -> order.add("SyncBefore A: " + s))
				.doBeforeRetry(s -> order.add("SyncBefore B, tracking " + beforeHookTracker.incrementAndGet()))
				.doAfterRetry(s -> order.add("SyncAfter A: " + s))
				.doAfterRetry(s -> order.add("SyncAfter B, tracking " + afterHookTracker.incrementAndGet()))
				.doBeforeRetryAsync(s -> Mono.delay(Duration.ofMillis(200)).doOnNext(n -> order.add("AsyncBefore C")).then())
				.doBeforeRetryAsync(s -> Mono.fromRunnable(() -> {
					order.add("AsyncBefore D");
					beforeHookTracker.addAndGet(100);
				}))
				.doAfterRetryAsync(s -> Mono.delay(Duration.ofMillis(150)).doOnNext(delayed -> order.add("AsyncAfter C " + s)).then())
				.doAfterRetryAsync(s -> Mono.fromRunnable(() -> {
					order.add("AsyncAfter D");
					afterHookTracker.addAndGet(100);
				}));

		Mono.error(new IllegalStateException("boom"))
		    .retryWhen(retryBuilder)
		    .as(StepVerifier::create)
		    .verifyError();

		assertThat(beforeHookTracker).hasValue(101);
		assertThat(afterHookTracker).hasValue(101);

		assertThat(order).containsExactly(
				"SyncBefore A: attempt #1 (1 in a row), last failure={java.lang.IllegalStateException: boom}",
				"SyncBefore B, tracking 1",
				"AsyncBefore C",
				"AsyncBefore D",
				"SyncAfter A: attempt #1 (1 in a row), last failure={java.lang.IllegalStateException: boom}",
				"SyncAfter B, tracking 1",
				"AsyncAfter C attempt #1 (1 in a row), last failure={java.lang.IllegalStateException: boom}",
				"AsyncAfter D"
		);
	}

	@Test
	public void cumulatedRetryHooksWithTransient() {
		List<String> order = new CopyOnWriteArrayList<>();
		AtomicInteger beforeHookTracker = new AtomicInteger();
		AtomicInteger afterHookTracker = new AtomicInteger();

		RetryBackoffSpec retryBuilder = Retry
				.backoff(2, Duration.ZERO)
				.maxBackoff(Duration.ZERO)
				.transientErrors(true)
				.doBeforeRetry(s -> order.add("SyncBefore A: " + s))
				.doBeforeRetry(s -> order.add("SyncBefore B, tracking " + beforeHookTracker.incrementAndGet()))
				.doAfterRetry(s -> order.add("SyncAfter A: " + s))
				.doAfterRetry(s -> order.add("SyncAfter B, tracking " + afterHookTracker.incrementAndGet()))
				.doBeforeRetryAsync(s -> Mono.delay(Duration.ofMillis(200)).doOnNext(n -> order.add("AsyncBefore C")).then())
				.doBeforeRetryAsync(s -> Mono.fromRunnable(() -> {
					order.add("AsyncBefore D");
					beforeHookTracker.addAndGet(100);
				}))
				.doAfterRetryAsync(s -> Mono.delay(Duration.ofMillis(150)).doOnNext(delayed -> order.add("AsyncAfter C " + s)).then())
				.doAfterRetryAsync(s -> Mono.fromRunnable(() -> {
					order.add("AsyncAfter D");
					afterHookTracker.addAndGet(100);
				}));

		FluxRetryWhenTest.transientErrorSource()
		                 .retryWhen(retryBuilder)
		                 .blockLast();

		assertThat(beforeHookTracker).as("before hooks cumulated").hasValue(606);
		assertThat(afterHookTracker).as("after hooks cumulated").hasValue(606);

		assertThat(order).containsExactly(
				"SyncBefore A: attempt #1 (1 in a row), last failure={java.lang.IllegalStateException: failing on step 1}",
				"SyncBefore B, tracking 1",
				"AsyncBefore C",
				"AsyncBefore D",
				"SyncAfter A: attempt #1 (1 in a row), last failure={java.lang.IllegalStateException: failing on step 1}",
				"SyncAfter B, tracking 1",
				"AsyncAfter C attempt #1 (1 in a row), last failure={java.lang.IllegalStateException: failing on step 1}",
				"AsyncAfter D",

				"SyncBefore A: attempt #2 (2 in a row), last failure={java.lang.IllegalStateException: failing on step 2}",
				"SyncBefore B, tracking 102",
				"AsyncBefore C",
				"AsyncBefore D",
				"SyncAfter A: attempt #2 (2 in a row), last failure={java.lang.IllegalStateException: failing on step 2}",
				"SyncAfter B, tracking 102",
				"AsyncAfter C attempt #2 (2 in a row), last failure={java.lang.IllegalStateException: failing on step 2}",
				"AsyncAfter D",

				"SyncBefore A: attempt #3 (1 in a row), last failure={java.lang.IllegalStateException: failing on step 5}",
				"SyncBefore B, tracking 203",
				"AsyncBefore C",
				"AsyncBefore D",
				"SyncAfter A: attempt #3 (1 in a row), last failure={java.lang.IllegalStateException: failing on step 5}",
				"SyncAfter B, tracking 203",
				"AsyncAfter C attempt #3 (1 in a row), last failure={java.lang.IllegalStateException: failing on step 5}",
				"AsyncAfter D",

				"SyncBefore A: attempt #4 (2 in a row), last failure={java.lang.IllegalStateException: failing on step 6}",
				"SyncBefore B, tracking 304",
				"AsyncBefore C",
				"AsyncBefore D",
				"SyncAfter A: attempt #4 (2 in a row), last failure={java.lang.IllegalStateException: failing on step 6}",
				"SyncAfter B, tracking 304",
				"AsyncAfter C attempt #4 (2 in a row), last failure={java.lang.IllegalStateException: failing on step 6}",
				"AsyncAfter D",

				"SyncBefore A: attempt #5 (1 in a row), last failure={java.lang.IllegalStateException: failing on step 9}",
				"SyncBefore B, tracking 405",
				"AsyncBefore C",
				"AsyncBefore D",
				"SyncAfter A: attempt #5 (1 in a row), last failure={java.lang.IllegalStateException: failing on step 9}",
				"SyncAfter B, tracking 405",
				"AsyncAfter C attempt #5 (1 in a row), last failure={java.lang.IllegalStateException: failing on step 9}",
				"AsyncAfter D",

				"SyncBefore A: attempt #6 (2 in a row), last failure={java.lang.IllegalStateException: failing on step 10}",
				"SyncBefore B, tracking 506",
				"AsyncBefore C",
				"AsyncBefore D",
				"SyncAfter A: attempt #6 (2 in a row), last failure={java.lang.IllegalStateException: failing on step 10}",
				"SyncAfter B, tracking 506",
				"AsyncAfter C attempt #6 (2 in a row), last failure={java.lang.IllegalStateException: failing on step 10}",
				"AsyncAfter D"
		);
	}

	@Test
	public void backoffSchedulerDefaultIsLazilyResolved() {
		RetryBackoffSpec spec = Retry.backoff(3, Duration.ofMillis(10));

		StepVerifier.withVirtualTime(() -> Flux.error(new IllegalStateException("boom"))
		                                       .retryWhen(spec)
		)
		            .expectSubscription()
		            .thenAwait(Duration.ofHours(1))
		            .expectErrorMatches(Exceptions::isRetryExhausted)
		            .verify(Duration.ofSeconds(1));
	}

	@Test
	public void backoffSchedulerIsEagerlyCaptured() {
		RetryBackoffSpec spec = Retry.backoff(3, Duration.ofMillis(500))
				.scheduler(Schedulers.parallel());

		Schedulers.resetFactory();

		assertThat(spec.backoffSchedulerSupplier.get())
				.isNotSameAs(Schedulers.parallel())
				.matches(Scheduler::isDisposed, "disposed");
	}

	@Test
	public void backoffSchedulerNullResetToDefaultSupplier() {
		RetryBackoffSpec specTemplate = Retry.backoff(3, Duration.ofMillis(10))
				.scheduler(Schedulers.single());

		RetryBackoffSpec spec = specTemplate.scheduler(null);

		assertThat(spec.backoffSchedulerSupplier.get()).isSameAs(Schedulers.parallel());
	}

}
