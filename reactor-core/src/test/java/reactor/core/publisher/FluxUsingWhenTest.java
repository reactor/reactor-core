/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.logging.Level;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable.Attr;
import reactor.core.publisher.FluxUsingWhen.ResourceSubscriber;
import reactor.core.publisher.FluxUsingWhen.UsingWhenSubscriber;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.TestLogger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public class FluxUsingWhenTest {

	@Test
	public void nullResourcePublisherRejected() {
		assertThatNullPointerException()
				.isThrownBy(() -> Flux.usingWhen(null,
						tr -> Mono.empty(),
						tr -> Mono.empty(),
						(tr, err) -> Mono.empty(),
						tr -> Mono.empty()))
				.withMessage("resourceSupplier")
				.withNoCause();
	}

	@Test
	public void emptyResourcePublisherDoesntApplyCallback() {
		AtomicBoolean commitDone = new AtomicBoolean();
		AtomicBoolean rollbackDone = new AtomicBoolean();

		Flux<String> test = Flux.usingWhen(Flux.empty().hide(),
				tr -> Mono.just("unexpected"),
				tr -> Mono.fromRunnable(() -> commitDone.set(true)),
				(tr, err) -> Mono.fromRunnable(() -> rollbackDone.set(true)),
				tr -> Mono.fromRunnable(() -> rollbackDone.set(true)));

		StepVerifier.create(test)
		            .verifyComplete();

		assertThat(commitDone).isFalse();
		assertThat(rollbackDone).isFalse();
	}

	@Test
	public void emptyResourceCallableDoesntApplyCallback() {
		AtomicBoolean commitDone = new AtomicBoolean();
		AtomicBoolean rollbackDone = new AtomicBoolean();

		Flux<String> test = Flux.usingWhen(Flux.empty(),
				tr -> Mono.just("unexpected"),
				tr -> Mono.fromRunnable(() -> commitDone.set(true)),
				(tr, err) -> Mono.fromRunnable(() -> rollbackDone.set(true)),
				tr -> Mono.fromRunnable(() -> rollbackDone.set(true)));

		StepVerifier.create(test)
		            .verifyComplete();

		assertThat(commitDone).isFalse();
		assertThat(rollbackDone).isFalse();
	}

	@Test
	public void errorResourcePublisherDoesntApplyCallback() {
		AtomicBoolean commitDone = new AtomicBoolean();
		AtomicBoolean rollbackDone = new AtomicBoolean();

		Flux<String> test = Flux.usingWhen(Flux.error(new IllegalStateException("boom")).hide(),
				tr -> Mono.just("unexpected"),
				tr -> Mono.fromRunnable(() -> commitDone.set(true)),
				(tr, err) -> Mono.fromRunnable(() -> rollbackDone.set(true)),
				tr -> Mono.fromRunnable(() -> rollbackDone.set(true)));

		StepVerifier.create(test)
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("boom")
				            .hasNoCause()
				            .hasNoSuppressedExceptions()
		            );

		assertThat(commitDone).isFalse();
		assertThat(rollbackDone).isFalse();
	}

	@Test
	public void errorResourceCallableDoesntApplyCallback() {
		AtomicBoolean commitDone = new AtomicBoolean();
		AtomicBoolean rollbackDone = new AtomicBoolean();

		Flux<String> test = Flux.usingWhen(Flux.error(new IllegalStateException("boom")),
				tr -> Mono.just("unexpected"),
				tr -> Mono.fromRunnable(() -> commitDone.set(true)),
				(tr, err) -> Mono.fromRunnable(() -> rollbackDone.set(true)),
				tr -> Mono.fromRunnable(() -> rollbackDone.set(true)));

		StepVerifier.create(test)
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("boom")
				            .hasNoCause()
				            .hasNoSuppressedExceptions()
		            );

		assertThat(commitDone).isFalse();
		assertThat(rollbackDone).isFalse();
	}

	@Test
	public void errorResourcePublisherAfterEmitIsDropped() {
		AtomicBoolean commitDone = new AtomicBoolean();
		AtomicBoolean rollbackDone = new AtomicBoolean();

		TestPublisher<String> testPublisher = TestPublisher.createColdNonCompliant(false, TestPublisher.Violation.DEFER_CANCELLATION);
		testPublisher.next("Resource").error(new IllegalStateException("boom"));

		Flux<String> test = Flux.usingWhen(testPublisher,
				Mono::just,
				tr -> Mono.fromRunnable(() -> commitDone.set(true)),
				(tr, err) -> Mono.fromRunnable(() -> rollbackDone.set(true)),
				tr -> Mono.fromRunnable(() -> rollbackDone.set(true)));

		StepVerifier.create(test)
		            .expectNext("Resource")
		            .expectComplete()
		            .verifyThenAssertThat(Duration.ofSeconds(2))
		            .hasDroppedErrorWithMessage("boom")
		            .hasNotDroppedElements();

		assertThat(commitDone).isTrue();
		assertThat(rollbackDone).isFalse();

		testPublisher.assertCancelled();
	}

	@Test
	public void secondResourceInPublisherIsDropped() {
		AtomicBoolean commitDone = new AtomicBoolean();
		AtomicBoolean rollbackDone = new AtomicBoolean();

		TestPublisher<String> testPublisher = TestPublisher.createColdNonCompliant(false, TestPublisher.Violation.DEFER_CANCELLATION);
		testPublisher.emit("Resource", "boom");

		Flux<String> test = Flux.usingWhen(testPublisher,
				Mono::just,
				tr -> Mono.fromRunnable(() -> commitDone.set(true)),
				(tr, err) -> Mono.fromRunnable(() -> rollbackDone.set(true)),
				tr -> Mono.fromRunnable(() -> rollbackDone.set(true)));

		StepVerifier.create(test)
		            .expectNext("Resource")
		            .expectComplete()
		            .verifyThenAssertThat(Duration.ofSeconds(2))
		            .hasDropped("boom")
		            .hasNotDroppedErrors();

		assertThat(commitDone).isTrue();
		assertThat(rollbackDone).isFalse();

		testPublisher.assertCancelled();
	}

	@Test
	public void fluxResourcePublisherIsCancelled() {
		AtomicBoolean cancelled = new AtomicBoolean();
		AtomicBoolean commitDone = new AtomicBoolean();
		AtomicBoolean rollbackDone = new AtomicBoolean();

		Flux<String> resourcePublisher = Flux.just("Resource", "Something Else")
		                                     .doOnCancel(() -> cancelled.set(true));

		Flux<String> test = Flux.usingWhen(resourcePublisher,
				Mono::just,
				tr -> Mono.fromRunnable(() -> commitDone.set(true)),
				(tr, err) -> Mono.fromRunnable(() -> rollbackDone.set(true)),
				tr -> Mono.fromRunnable(() -> rollbackDone.set(true)));

		StepVerifier.create(test)
		            .expectNext("Resource")
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasNotDroppedErrors();

		assertThat(commitDone).isTrue();
		assertThat(rollbackDone).isFalse();

		assertThat(cancelled).as("resource publisher was cancelled").isTrue();
	}

	@Test
	public void monoResourcePublisherIsNotCancelled() {
		AtomicBoolean cancelled = new AtomicBoolean();
		AtomicBoolean commitDone = new AtomicBoolean();
		AtomicBoolean rollbackDone = new AtomicBoolean();

		Mono<String> resourcePublisher = Mono.just("Resource")
		                                     .doOnCancel(() -> cancelled.set(true));

		Flux<String> test = Flux.usingWhen(resourcePublisher,
				Flux::just,
				tr -> Mono.fromRunnable(() -> commitDone.set(true)),
				(tr, err) -> Mono.fromRunnable(() -> rollbackDone.set(true)),
				tr -> Mono.fromRunnable(() -> rollbackDone.set(true)));

		StepVerifier.create(test)
		            .expectNext("Resource")
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasNotDroppedErrors();

		assertThat(commitDone).isTrue();
		assertThat(rollbackDone).isFalse();

		assertThat(cancelled).as("resource publisher was not cancelled").isFalse();
	}

	@Test
	public void lateFluxResourcePublisherIsCancelledOnCancel() {
		AtomicBoolean resourceCancelled = new AtomicBoolean();
		AtomicBoolean commitDone = new AtomicBoolean();
		AtomicBoolean rollbackDone = new AtomicBoolean();
		AtomicBoolean cancelDone = new AtomicBoolean();

		Flux<String> resourcePublisher = Flux.<String>never()
		                                     .doOnCancel(() -> resourceCancelled.set(true));

		StepVerifier.create(Flux.usingWhen(resourcePublisher,
				Flux::just,
				tr -> Mono.fromRunnable(() -> commitDone.set(true)),
				(tr, err) -> Mono.fromRunnable(() -> rollbackDone.set(true)),
				tr -> Mono.fromRunnable(() -> cancelDone.set(true))))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenCancel()
		            .verify(Duration.ofSeconds(1));

		assertThat(commitDone).as("commitDone").isFalse();
		assertThat(rollbackDone).as("rollbackDone").isFalse();
		assertThat(cancelDone).as("cancelDone").isFalse();

		assertThat(resourceCancelled).as("resource cancelled").isTrue();
	}

	@Test
	public void lateMonoResourcePublisherIsCancelledOnCancel() {
		AtomicBoolean resourceCancelled = new AtomicBoolean();
		AtomicBoolean commitDone = new AtomicBoolean();
		AtomicBoolean rollbackDone = new AtomicBoolean();
		AtomicBoolean cancelDone = new AtomicBoolean();

		Mono<String> resourcePublisher = Mono.<String>never()
				.doOnCancel(() -> resourceCancelled.set(true));

		Mono<String> usingWhen = Mono.usingWhen(resourcePublisher,
				Mono::just,
				tr -> Mono.fromRunnable(() -> commitDone.set(true)),
				(tr, err) -> Mono.fromRunnable(() -> rollbackDone.set(true)),
				tr -> Mono.fromRunnable(() -> cancelDone.set(true)));

		StepVerifier.create(usingWhen)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenCancel()
		            .verify(Duration.ofSeconds(1));

		assertThat(commitDone).as("commitDone").isFalse();
		assertThat(rollbackDone).as("rollbackDone").isFalse();
		assertThat(cancelDone).as("cancelDone").isFalse();

		assertThat(resourceCancelled).as("resource cancelled").isTrue();
	}

	@Test
	public void blockOnNeverResourceCanBeCancelled() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		Disposable disposable = Flux.usingWhen(Flux.<String>never(),
				Flux::just,
				Flux::just,
				(res, err) -> Flux.just(res),
				Flux::just)
		                            .doFinally(f -> latch.countDown())
		                            .subscribe();

		assertThat(latch.await(500, TimeUnit.MILLISECONDS))
				.as("hangs before dispose").isFalse();

		disposable.dispose();

		assertThat(latch.await(100, TimeUnit.MILLISECONDS))
				.as("terminates after dispose").isTrue();
	}

	@Test
	public void failToGenerateClosureAppliesRollback() {
		TestResource testResource = new TestResource();

		Flux<String> test = Flux.usingWhen(Mono.just(testResource),
				tr -> {
					throw new UnsupportedOperationException("boom");
				},
				TestResource::commit,
				TestResource::rollback,
				TestResource::cancel);

		StepVerifier.create(test)
		            .verifyErrorSatisfies(e -> assertThat(e).hasMessage("boom"));

		testResource.commitProbe.assertWasNotSubscribed();
		testResource.cancelProbe.assertWasNotSubscribed();
		testResource.rollbackProbe.assertWasSubscribed();
	}

	@Test
	public void nullClosureAppliesRollback() {
		TestResource testResource = new TestResource();

		Flux<String> test = Flux.usingWhen(Mono.just(testResource),
				tr -> null,
				TestResource::commit,
				TestResource::rollback,
				TestResource::cancel);

		StepVerifier.create(test)
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(NullPointerException.class)
				            .hasMessage("The resourceClosure function returned a null value"));

		testResource.commitProbe.assertWasNotSubscribed();
		testResource.cancelProbe.assertWasNotSubscribed();
		testResource.rollbackProbe.assertWasSubscribed();
	}

	@ParameterizedTest
	@MethodSource("sources01")
	public void cancelWithHandler(Flux<String> source) {
		TestResource testResource = new TestResource();

		Flux<String> test = Flux.usingWhen(Mono.just(testResource),
				tr -> source,
				TestResource::commit,
				TestResource::rollback,
				TestResource::cancel)
		                        .take(2);

		StepVerifier.create(test)
		            .expectNext("0", "1")
		            .verifyComplete();

		testResource.commitProbe.assertWasNotSubscribed();
		testResource.rollbackProbe.assertWasNotSubscribed();
		testResource.cancelProbe.assertWasSubscribed();
	}

	@ParameterizedTest
	@MethodSource("sources01")
	public void cancelWithHandlerFailure(Flux<String> source) {
		TestResource testResource = new TestResource();
		final TestLogger tl = new TestLogger();
		Loggers.useCustomLoggers(name -> tl);

		try {
			Flux<String> test = Flux.usingWhen(Mono.just(testResource),
					tr -> source,
					TestResource::commit,
					TestResource::rollback,
					r -> r.cancel()
					      //immediate error to trigger the logging within the test
					      .concatWith(Mono.error(new IllegalStateException("cancel error")))
			)
			                        .take(2);

			StepVerifier.create(test)
			            .expectNext("0", "1")
			            .verifyComplete();

			testResource.commitProbe.assertWasNotSubscribed();
			testResource.rollbackProbe.assertWasNotSubscribed();
			testResource.cancelProbe.assertWasSubscribed();
		}
		finally {
			Loggers.resetLoggerFactory();
		}
		assertThat(tl.getErrContent())
				.contains("Async resource cleanup failed after cancel")
				.contains("java.lang.IllegalStateException: cancel error");
	}

	@ParameterizedTest
	@MethodSource("sources01")
	public void cancelWithHandlerGenerationFailureLogs(Flux<String> source) throws InterruptedException {
		TestLogger tl = new TestLogger();
		Loggers.useCustomLoggers(name -> tl);
		TestResource testResource = new TestResource();

		try {
			Flux<String> test = Flux.usingWhen(Mono.just(testResource),
					tr -> source,
					TestResource::commit,
					TestResource::rollback,
					r -> null)
			                        .take(2);

			StepVerifier.create(test)
			            .expectNext("0", "1")
			            .verifyComplete();

			testResource.commitProbe.assertWasNotSubscribed();
			testResource.cancelProbe.assertWasNotSubscribed();
			testResource.rollbackProbe.assertWasNotSubscribed();
		}
		finally {
			Loggers.resetLoggerFactory();
		}
		assertThat(tl.getErrContent())
				.contains("Error generating async resource cleanup during onCancel")
				.contains("java.lang.NullPointerException");
	}

	@ParameterizedTest
	@MethodSource("sources01")
	@Deprecated
	public void cancelWithoutHandlerAppliesCommit(Flux<String> source) {
		TestResource testResource = new TestResource();

		Flux<String> test = Flux
				.usingWhen(
						Mono.just(testResource).hide(),
						tr -> source,
						TestResource::commit,
						(tr, e) -> tr.rollback(new RuntimeException("placeholder rollback exception")),
						TestResource::commit
				)
				.take(2);

		StepVerifier.create(test)
		            .expectNext("0", "1")
		            .verifyComplete();

		testResource.commitProbe.assertWasSubscribed();
		testResource.cancelProbe.assertWasNotSubscribed();
		testResource.rollbackProbe.assertWasNotSubscribed();
	}

	@ParameterizedTest
	@MethodSource("sources01")
	@Deprecated
	public void cancelDefaultHandlerFailure(Flux<String> source) {
		TestResource testResource = new TestResource();
		final TestLogger tl = new TestLogger();
		Loggers.useCustomLoggers(name -> tl);

		try {
			Function<TestResource, Publisher<?>> completeOrCancel = r -> {
				return r.commit()
				        //immediate error to trigger the logging within the test
				        .concatWith(Mono.error(new IllegalStateException("commit error")));
			};
			Flux<String> test = Flux
					.usingWhen(
							Mono.just(testResource),
							tr -> source,
							completeOrCancel,
							(r, e) -> r.rollback(new RuntimeException("placeholder ignored rollback exception")),
							completeOrCancel
					)
                    .take(2);

			StepVerifier.create(test)
			            .expectNext("0", "1")
			            .verifyComplete();

			testResource.commitProbe.assertWasSubscribed();
			testResource.cancelProbe.assertWasNotSubscribed();
			testResource.rollbackProbe.assertWasNotSubscribed();
		}
		finally {
			Loggers.resetLoggerFactory();
		}
		assertThat(tl.getErrContent())
				.contains("Async resource cleanup failed after cancel")
				.contains("java.lang.IllegalStateException: commit error");
	}

	@ParameterizedTest
	@MethodSource("sourcesFullTransaction")
	public void apiCommit(Flux<String> fullTransaction) {
		final AtomicReference<TestResource> ref = new AtomicReference<>();

		Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
				d -> {
					ref.set(d);
					return fullTransaction;
				},
				TestResource::commit,
				TestResource::rollback,
				TestResource::cancel);

		StepVerifier.create(flux)
		            .expectNext("Transaction started")
		            .expectNext("work in transaction")
		            .expectNext("more work in transaction")
		            .expectComplete()
		            .verify();

		assertThat(ref.get())
				.isNotNull()
				.matches(tr -> tr.commitProbe.wasSubscribed(), "commit method used")
				.matches(tr -> !tr.cancelProbe.wasSubscribed(), "no cancel")
				.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "no rollback");
	}

	@ParameterizedTest
	@MethodSource("sourcesFullTransaction")
	public void apiCommitFailure(Flux<String> fullTransaction) {
		final AtomicReference<TestResource> ref = new AtomicReference<>();

		Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
				d -> {
					ref.set(d);
					return fullTransaction;
				},
				TestResource::commitError,
				TestResource::rollback,
				TestResource::cancel);

		StepVerifier.create(flux)
		            .expectNext("Transaction started")
		            .expectNext("work in transaction")
		            .expectNext("more work in transaction")
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .hasMessage("Async resource cleanup failed after onComplete")
				            .hasCauseInstanceOf(ArithmeticException.class));

		assertThat(ref.get())
				.isNotNull()
				.matches(tr -> tr.commitProbe.wasSubscribed(), "commit method used")
				.matches(tr -> !tr.cancelProbe.wasSubscribed(), "no cancel")
				.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "no rollback");
	}

	@ParameterizedTest
	@MethodSource("sourcesFullTransaction")
	public void commitGeneratingNull(Flux<String> fullTransaction) {
		final AtomicReference<TestResource> ref = new AtomicReference<>();

		Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
				d -> {
					ref.set(d);
					return fullTransaction;
				},
				TestResource::commitNull,
				TestResource::rollback,
				TestResource::cancel);

		StepVerifier.create(flux)
		            .expectNext("Transaction started")
		            .expectNext("work in transaction")
		            .expectNext("more work in transaction")
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .hasMessage("The asyncComplete returned a null Publisher")
				            .isInstanceOf(NullPointerException.class)
				            .hasNoCause());

		assertThat(ref.get())
				.isNotNull()
				.matches(tr -> !tr.commitProbe.wasSubscribed(), "commit method short-circuited")
				.matches(tr -> !tr.cancelProbe.wasSubscribed(), "no cancel")
				.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "no rollback");
	}

	@ParameterizedTest
	@MethodSource("sourcesTransactionError")
	public void apiRollback(Flux<String> transactionWithError) {
		final AtomicReference<TestResource> ref = new AtomicReference<>();
		Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
				d -> {
					ref.set(d);
					return transactionWithError;
				},
				TestResource::commitError,
				TestResource::rollback,
				TestResource::cancel);

		StepVerifier.create(flux)
		            .expectNext("Transaction started")
		            .expectNext("work in transaction")
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .hasMessage("boom")
				            .hasNoCause()
				            .hasNoSuppressedExceptions());

		assertThat(ref.get())
				.isNotNull()
				.matches(tr -> !tr.commitProbe.wasSubscribed(), "no commit")
				.matches(tr -> !tr.cancelProbe.wasSubscribed(), "no cancel")
				.matches(tr -> tr.rollbackProbe.wasSubscribed(), "rollback method used");
	}

	@ParameterizedTest
	@MethodSource("sourcesTransactionError")
	public void apiRollbackFailure(Flux<String> transactionWithError) {
		final AtomicReference<TestResource> ref = new AtomicReference<>();
		Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
				d -> {
					ref.set(d);
					return transactionWithError;
				},
				TestResource::commitError,
				TestResource::rollbackError,
				TestResource::cancel);

		StepVerifier.create(flux)
		            .expectNext("Transaction started")
		            .expectNext("work in transaction")
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .hasMessage("Async resource cleanup failed after onError")
				            .hasCauseInstanceOf(ArithmeticException.class)
				            .hasSuppressedException(new IllegalStateException("boom")));

		assertThat(ref.get())
				.isNotNull()
				.matches(tr -> !tr.commitProbe.wasSubscribed(), "no commit")
				.matches(tr -> !tr.cancelProbe.wasSubscribed(), "no cancel")
				.matches(tr -> tr.rollbackProbe.wasSubscribed(), "rollback method used");
	}

	@ParameterizedTest
	@MethodSource("sourcesTransactionError")
	public void apiRollbackGeneratingNull(Flux<String> transactionWithError) {
		final AtomicReference<TestResource> ref = new AtomicReference<>();
		Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
				d -> {
					ref.set(d);
					return transactionWithError;
				},
				TestResource::commitError,
				TestResource::rollbackNull,
				TestResource::cancel);

		StepVerifier.create(flux)
		            .expectNext("Transaction started")
		            .expectNext("work in transaction")
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .hasMessage("The asyncError returned a null Publisher")
				            .isInstanceOf(NullPointerException.class)
				            .hasSuppressedException(new IllegalStateException("boom")));

		assertThat(ref.get())
				.isNotNull()
				.matches(tr -> !tr.commitProbe.wasSubscribed(), "no commit")
				.matches(tr -> !tr.cancelProbe.wasSubscribed(), "no cancel")
				.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "rollback method short-circuited");
	}

	@ParameterizedTest
	@MethodSource("sourcesFullTransaction")
	public void apiCancel(Flux<String> transactionWithError) {
		final AtomicReference<TestResource> ref = new AtomicReference<>();
		Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
				d -> {
					ref.set(d);
					return transactionWithError;
				},
				TestResource::commit,
				TestResource::rollback,
				TestResource::cancel);

		StepVerifier.create(flux.take(1), 1)
		            .expectNext("Transaction started")
		            .verifyComplete();

		assertThat(ref.get())
				.isNotNull()
				.matches(tr -> !tr.commitProbe.wasSubscribed(), "no commit")
				.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "no rollback")
				.matches(tr -> tr.cancelProbe.wasSubscribed(), "cancel method used");
	}

	@ParameterizedTest
	@MethodSource("sourcesFullTransaction")
	public void apiCancelFailure(Flux<String> transaction) {
		TestLogger testLogger = new TestLogger();
		Loggers.useCustomLoggers(s -> testLogger);
		try {
			final AtomicReference<TestResource> ref = new AtomicReference<>();
			Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
					d -> {
						ref.set(d);
						return transaction;
					},
					TestResource::commit,
					TestResource::rollback,
					TestResource::cancelError);

			StepVerifier.create(flux.take(1), 1)
			            .expectNext("Transaction started")
			            .verifyComplete();

			assertThat(ref.get())
					.isNotNull()
					.matches(tr -> !tr.commitProbe.wasSubscribed(), "no commit")
					.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "no rollback")
					.matches(tr -> tr.cancelProbe.wasSubscribed(), "cancel method used");

			//since the CancelInner is subscribed in a fire-and-forget fashion, the log comes later
			//the test must be done before the finally, lest the error message be printed too late for TestLogger to catch it
			Awaitility.await().atMost(1, TimeUnit.SECONDS)
			          .untilAsserted(() ->
					          assertThat(testLogger.getErrContent())
							          .startsWith("[ WARN]")
							          .contains("Async resource cleanup failed after cancel - java.lang.ArithmeticException: / by zero"));
		}
		finally {
			Loggers.resetLoggerFactory();
		}
	}

	@ParameterizedTest
	@MethodSource("sourcesFullTransaction")
	public void apiCancelGeneratingNullLogs(Flux<String> transactionWithError) {
		TestLogger testLogger = new TestLogger();
		Loggers.useCustomLoggers(s -> testLogger);
		try {
			final AtomicReference<TestResource> ref = new AtomicReference<>();
			Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
					d -> {
						ref.set(d);
						return transactionWithError;
					},
					TestResource::commit,
					TestResource::rollback,
					TestResource::cancelNull);

			StepVerifier.create(flux.take(1), 1)
			            .expectNext("Transaction started")
			            .verifyComplete();

			assertThat(ref.get())
					.isNotNull()
					.matches(tr -> !tr.commitProbe.wasSubscribed(), "no commit")
					.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "no rollback")
					.matches(tr -> !tr.cancelProbe.wasSubscribed(), "cancel method short-circuited");

		}
		finally {
			Loggers.resetLoggerFactory();
		}
		assertThat(testLogger.getErrContent())
				.contains("[ WARN] (" + Thread.currentThread().getName() + ") " +
						"Error generating async resource cleanup during onCancel - java.lang.NullPointerException");
	}

	@Test
	@Deprecated
	public void apiSingleAsyncCleanup() {
		final AtomicReference<TestResource> ref = new AtomicReference<>();

		Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
				d -> {
					ref.set(d);
					return d.data().concatWithValues("work in transaction");
				},
				TestResource::commit);

		StepVerifier.create(flux)
		            .expectNext("Transaction started")
		            .expectNext("work in transaction")
		            .verifyComplete();

		assertThat(ref.get())
				.isNotNull()
				.matches(tr -> tr.commitProbe.wasSubscribed(), "commit method used")
				.matches(tr -> !tr.cancelProbe.wasSubscribed(), "no cancel")
				.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "no rollback");
	}

	@Test
	@Deprecated
	public void apiSingleAsyncCleanupFailure() {
		final RuntimeException rollbackCause = new IllegalStateException("boom");
		final AtomicReference<TestResource> ref = new AtomicReference<>();

		Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
				d -> {
					ref.set(d);
					return d.data().concatWithValues("work in transaction")
					        .concatWith(Mono.error(rollbackCause));
				},
				TestResource::commitError);

		StepVerifier.create(flux)
		            .expectNext("Transaction started")
		            .expectNext("work in transaction")
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .hasMessage("Async resource cleanup failed after onError")
				            .hasCauseInstanceOf(ArithmeticException.class)
				            .hasSuppressedException(rollbackCause));

		assertThat(ref.get())
				.isNotNull()
				.matches(tr -> tr.commitProbe.wasSubscribed(), "commit method used despite error")
				.matches(tr -> !tr.cancelProbe.wasSubscribed(), "no cancel")
				.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "no rollback");
	}

	@Test
	public void normalHasNoQueueOperations() {
		final FluxPeekFuseableTest.AssertQueueSubscription<String> assertQueueSubscription =
				new FluxPeekFuseableTest.AssertQueueSubscription<>();
		assertQueueSubscription.offer("foo");

		UsingWhenSubscriber<String, String>
				test = new UsingWhenSubscriber<>(new LambdaSubscriber<>(null, null, null, null),
				"resource", it -> Mono.empty(), (it, err) -> Mono.empty(), null, Mockito.mock(Operators.DeferredSubscription.class));

		test.onSubscribe(assertQueueSubscription);

		assertThat(test).isNotInstanceOf(Fuseable.QueueSubscription.class);
	}

	@ParameterizedTest
	@MethodSource("sourcesContext")
	public void contextPropagationOnCommit(Mono<String> source) {
		AtomicReference<String> probeContextValue = new AtomicReference<>();
		AtomicReference<String> resourceContextValue = new AtomicReference<>();

		TestResource testResource = new TestResource();
		PublisherProbe<String> probe = PublisherProbe.of(
				Mono.deferContextual(Mono::just)
				    .map(it -> it.get(String.class))
				    .doOnNext(probeContextValue::set)
				    .onErrorReturn("fail")
		);
		Mono<String> contextHandler = probe.mono();

		Mono<TestResource> resourceProvider = Mono.just(testResource)
		                                          .zipWith(Mono.deferContextual(Mono::just))
		                                          .doOnNext(it -> resourceContextValue.set(it.getT2().get(String.class)))
		                                          .map(Tuple2::getT1);

		Flux.usingWhen(resourceProvider,
				r -> source,
				r -> contextHandler,
				TestResource::rollback,
				TestResource::cancel)
		    .contextWrite(Context.of(String.class, "contextual"))
		    .as(StepVerifier::create)
		    .expectAccessibleContext().contains(String.class, "contextual")
		    .then()
		    .expectNext("contextual")
		    .verifyComplete();

		testResource.commitProbe.assertWasNotSubscribed();
		testResource.cancelProbe.assertWasNotSubscribed();
		testResource.rollbackProbe.assertWasNotSubscribed();
		probe.assertWasSubscribed();

		assertThat(probeContextValue).hasValue("contextual");
		assertThat(resourceContextValue).hasValue("contextual");
	}

	@ParameterizedTest
	@MethodSource("sourcesContextError")
	public void contextPropagationOnRollback(Mono<String> source) {
		AtomicReference<String> probeContextValue = new AtomicReference<>();
		AtomicReference<String> resourceContextValue = new AtomicReference<>();

		TestResource testResource = new TestResource();
		PublisherProbe<String> probe = PublisherProbe.of(
				Mono.deferContextual(Mono::just)
				    .map(it -> it.get(String.class))
				    .doOnNext(probeContextValue::set)
				    .onErrorReturn("fail")
		);
		Mono<String> contextHandler = probe.mono();

		Mono<TestResource> resourceProvider = Mono.just(testResource)
		                                          .zipWith(Mono.deferContextual(Mono::just))
		                                          .doOnNext(it -> resourceContextValue.set(it.getT2().get(String.class)))
		                                          .map(Tuple2::getT1);

		Flux.usingWhen(resourceProvider,
				r -> source,
				TestResource::commit,
				(r, err) -> contextHandler,
				TestResource::cancel)
		    .contextWrite(Context.of(String.class, "contextual"))
		    .as(StepVerifier::create)
		    .expectAccessibleContext().contains(String.class, "contextual")
		    .then()
		    .verifyErrorMessage("boom");

		testResource.commitProbe.assertWasNotSubscribed();
		testResource.cancelProbe.assertWasNotSubscribed();
		testResource.rollbackProbe.assertWasNotSubscribed();
		probe.assertWasSubscribed();

		assertThat(probeContextValue).hasValue("contextual");
		assertThat(resourceContextValue).hasValue("contextual");
	}

	@ParameterizedTest
	@MethodSource("sources01")
	public void contextPropagationOnCancel(Flux<String> source) {
		TestResource testResource = new TestResource();
		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		PublisherProbe<String> probe = PublisherProbe.of(
				Mono.deferContextual(Mono::just)
				    .map(it -> it.get(String.class))
				    .doOnError(errorRef::set)
				    .onErrorReturn("fail")
		);
		Mono<String> cancelHandler = probe.mono();

		Flux.usingWhen(Mono.just(testResource),
				r -> source,
				TestResource::commit,
				TestResource::rollback,
				cancel -> cancelHandler)
		    .contextWrite(Context.of(String.class, "contextual"))
		    .take(1)
		    .as(StepVerifier::create)
		    .expectNextCount(1)
		    .verifyComplete();

		testResource.rollbackProbe.assertWasNotSubscribed();
		testResource.commitProbe.assertWasNotSubscribed();
		probe.assertWasSubscribed();
		assertThat(errorRef).hasValue(null);
	}

	@ParameterizedTest
	@MethodSource("sources01")
	public void contextPropagationOnCancelWithNoHandler(Flux<String> source) {
		TestResource testResource = new TestResource();
		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		PublisherProbe<String> probe = PublisherProbe.of(
				Mono.deferContextual(Mono::just)
				    .map(it -> it.get(String.class))
				    .doOnError(errorRef::set)
				    .onErrorReturn("fail")
		);
		Mono<String> cancelHandler = probe.mono();

		new FluxUsingWhen<>(Mono.just(testResource),
				r -> source,
				commit -> cancelHandler,
				TestResource::rollback,
				null)
		    .contextWrite(Context.of(String.class, "contextual"))
		    .take(1)
		    .as(StepVerifier::create)
		    .expectNextCount(1)
		    .verifyComplete();

		testResource.rollbackProbe.assertWasNotSubscribed();
		testResource.commitProbe.assertWasNotSubscribed();
		probe.assertWasSubscribed();
		assertThat(errorRef).hasValue(null);
	}

	// == tests checking callbacks don't pile up ==

	@Test
	public void noCancelCallbackAfterComplete() {
		LongAdder cleanupCount = new LongAdder();
		Flux<String> flux = Flux.usingWhen(Mono.defer(() -> Mono.just("foo")), Mono::just,
				s -> Mono.fromRunnable(() -> cleanupCount.add(10)), //10 for completion
				(s, err) -> Mono.fromRunnable(() -> cleanupCount.add(100)), //100 for error
				s -> Mono.fromRunnable(() -> cleanupCount.add(1000)) //1000 for cancel
		);

		flux.subscribe(new CoreSubscriber<Object>() {
			Subscription subscription;

			@Override
			public void onSubscribe(Subscription s) {
				subscription = s;
				s.request(1);
			}

			@Override
			public void onNext(Object o) {}

			@Override
			public void onError(Throwable t) {}

			@Override
			public void onComplete() {
				subscription.cancel();
			}
		});

		assertThat(cleanupCount.sum()).isEqualTo(10);
	}

	@Test
	public void noCancelCallbackAfterError() {
		LongAdder cleanupCount = new LongAdder();
		Flux<String> flux = Flux.usingWhen(Mono.just("foo"), v -> Mono.error(new IllegalStateException("boom")),
				s -> Mono.fromRunnable(() -> cleanupCount.add(10)), //10 for completion
				(s, err) -> Mono.fromRunnable(() -> cleanupCount.add(100)), //100 for error
				s -> Mono.fromRunnable(() -> cleanupCount.add(1000)) //1000 for cancel
		);

		flux.subscribe(new CoreSubscriber<Object>() {
			Subscription subscription;

			@Override
			public void onSubscribe(Subscription s) {
				subscription = s;
				s.request(1);
			}

			@Override
			public void onNext(Object o) {}

			@Override
			public void onError(Throwable t) {
				subscription.cancel();
			}

			@Override
			public void onComplete() {}
		});

		assertThat(cleanupCount.sum()).isEqualTo(100);
	}

	@Test
	public void noCompleteCallbackAfterCancel() throws InterruptedException {
		AtomicBoolean cancelled = new AtomicBoolean();
		LongAdder cleanupCount = new LongAdder();

		Publisher<String> badPublisher = s -> s.onSubscribe(new Subscription() {
			@Override
			public void request(long n) {
				new Thread(() -> {
					s.onNext("foo1");
					try { Thread.sleep(100); } catch (InterruptedException e) {}
					s.onComplete();
				}).start();
			}

			@Override
			public void cancel() {
				cancelled.set(true);
			}
		});

		Flux<String> flux = Flux.usingWhen(Mono.just("foo"), v -> badPublisher,
				s -> Mono.fromRunnable(() -> cleanupCount.add(10)), //10 for completion
				(s, err) -> Mono.fromRunnable(() -> cleanupCount.add(100)), //100 for error
				s -> Mono.fromRunnable(() -> cleanupCount.add(1000)) //1000 for cancel
		);

		flux.subscribe(new CoreSubscriber<String>() {
			Subscription subscription;

			@Override
			public void onSubscribe(Subscription s) {
				subscription = s;
				s.request(1);
			}

			@Override
			public void onNext(String o) {
				subscription.cancel();
			}

			@Override
			public void onError(Throwable t) {}

			@Override
			public void onComplete() {}
		});

		Awaitility.waitAtMost(500, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> assertThat(cleanupCount.sum()).isEqualTo(1000));
		assertThat(cancelled).as("source cancelled").isTrue();
	}

	@Test
	public void noErrorCallbackAfterCancel() throws InterruptedException {
		AtomicBoolean cancelled = new AtomicBoolean();
		LongAdder cleanupCount = new LongAdder();

		Publisher<String> badPublisher = s -> s.onSubscribe(new Subscription() {
			@Override
			public void request(long n) {
				new Thread(() -> {
					s.onNext("foo1");
					try { Thread.sleep(100); } catch (InterruptedException e) {}
					s.onError(new IllegalStateException("boom"));
				}).start();
			}

			@Override
			public void cancel() {
				cancelled.set(true);
			}
		});

		Flux<String> flux = Flux.usingWhen(Mono.just("foo"), v -> badPublisher,
				s -> Mono.fromRunnable(() -> cleanupCount.add(10)), //10 for completion
				(s, err) -> Mono.fromRunnable(() -> cleanupCount.add(100)), //100 for error
				s -> Mono.fromRunnable(() -> cleanupCount.add(1000)) //1000 for cancel
		);

		flux.subscribe(new CoreSubscriber<String>() {
			Subscription subscription;

			@Override
			public void onSubscribe(Subscription s) {
				subscription = s;
				s.request(1);
			}

			@Override
			public void onNext(String o) {
				subscription.cancel();
			}

			@Override
			public void onError(Throwable t) {}

			@Override
			public void onComplete() {}
		});

		Awaitility.waitAtMost(500, TimeUnit.MILLISECONDS)
		          .untilAsserted(() -> assertThat(cleanupCount.sum()).isEqualTo(1000));
		assertThat(cancelled).as("source cancelled").isTrue();
	}

	@Test
	public void errorCallbackReceivesCause() {
		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		NullPointerException npe = new NullPointerException("original error");

		Flux.usingWhen(Flux.just("ignored"), s -> Flux.concat(Flux.just("ignored1", "ignored2"), Flux.error(npe)),
				Mono::just,
				(res, err) -> Mono.fromRunnable(() -> errorRef.set(err)),
				Mono::just)
		    .as(StepVerifier::create)
		    .expectNext("ignored1", "ignored2")
		    .verifyErrorSatisfies(e -> assertThat(e).isSameAs(npe)
		                                            .hasNoCause()
		                                            .hasNoSuppressedExceptions());

		assertThat(errorRef).hasValue(npe);
	}


	// == scanUnsafe tests ==

	@Test
	public void scanOperator() {
		FluxUsingWhen<Object, Object> op = new FluxUsingWhen<>(Mono.empty(), Mono::just, Mono::just, (s, err) -> Mono.just(s), Mono::just);

		assertThat(op.scan(Attr.RUN_STYLE)).isSameAs(Attr.RunStyle.SYNC);
	}

	@Test
	public void scanResourceSubscriber() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		ResourceSubscriber<String, Integer> op = new ResourceSubscriber<>(actual, s -> Flux.just(s.length()), Mono::just, (s, err) -> Mono.just(s), Mono::just, true);
		final Subscription parent = Operators.emptySubscription();
		op.onSubscribe(parent);

		assertThat(op.scan(Attr.PARENT)).as("PARENT").isSameAs(parent);
		assertThat(op.scan(Attr.ACTUAL)).as("ACTUAL").isSameAs(actual);

		assertThat(op.scan(Attr.PREFETCH)).as("PREFETCH").isEqualTo(Integer.MAX_VALUE);
		assertThat(op.scan(Attr.RUN_STYLE)).isSameAs(Attr.RunStyle.SYNC);

		assertThat(op.scan(Attr.TERMINATED)).as("TERMINATED").isFalse();
		op.resourceProvided = true;
		assertThat(op.scan(Attr.TERMINATED)).as("TERMINATED resourceProvided").isTrue();

		assertThat(op.scanUnsafe(Attr.CANCELLED)).as("CANCELLED not supported").isNull();
	}

	@Test
	public void scanUsingWhenSubscriber() {
		CoreSubscriber<? super Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		UsingWhenSubscriber<Integer, String> op = new UsingWhenSubscriber<>(actual, "RESOURCE", Mono::just, (s, err) -> Mono.just(s), Mono::just, null);
		final Subscription parent = Operators.emptySubscription();
		op.onSubscribe(parent);

		assertThat(op.scan(Attr.PARENT)).as("PARENT").isSameAs(parent);
		assertThat(op.scan(Attr.ACTUAL)).as("ACTUAL")
		                                .isSameAs(actual)
		                                .isSameAs(op.actual());
		assertThat(op.scan(Attr.RUN_STYLE)).isSameAs(Attr.RunStyle.SYNC);

		assertThat(op.scan(Attr.TERMINATED)).as("pre TERMINATED").isFalse();
		assertThat(op.scan(Attr.CANCELLED)).as("pre CANCELLED").isFalse();

		op.deferredError(new IllegalStateException("boom"));
		assertThat(op.scan(Attr.TERMINATED)).as("TERMINATED with error").isTrue();
		assertThat(op.scan(Attr.ERROR)).as("ERROR").hasMessage("boom");

		op.cancel();
		assertThat(op.scan(Attr.CANCELLED)).as("CANCELLED").isTrue();
	}

	@Test
	public void scanCommitInner() {
		CoreSubscriber<? super Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		UsingWhenSubscriber<Integer, String> up = new UsingWhenSubscriber<>(actual, "RESOURCE", Mono::just, (s, err) -> Mono.just(s), Mono::just, null);
		final Subscription parent = Operators.emptySubscription();
		up.onSubscribe(parent);

		FluxUsingWhen.CommitInner op = new FluxUsingWhen.CommitInner(up);

		assertThat(op.scan(Attr.PARENT)).as("PARENT").isSameAs(up);
		assertThat(op.scan(Attr.ACTUAL)).as("ACTUAL").isSameAs(up.actual);
		assertThat(op.scan(Attr.RUN_STYLE)).isSameAs(Attr.RunStyle.SYNC);

		assertThat(op.scan(Attr.TERMINATED)).as("TERMINATED before").isFalse();

		op.onError(new IllegalStateException("boom"));
		assertThat(op.scan(Attr.TERMINATED))
				.as("TERMINATED by error")
				.isSameAs(up.scan(Attr.TERMINATED))
				.isTrue();
		assertThat(up.scan(Attr.ERROR)).as("parent ERROR")
		                               .hasMessage("Async resource cleanup failed after onComplete")
		                               .hasCause(new IllegalStateException("boom"));

		assertThat(op.scanUnsafe(Attr.PREFETCH)).as("PREFETCH not supported").isNull();
	}

	@Test
	public void scanRollbackInner() {
		CoreSubscriber<? super Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		UsingWhenSubscriber<Integer, String> up = new UsingWhenSubscriber<>(actual, "RESOURCE", Mono::just, (s, err) -> Mono.just(s), Mono::just, null);
		final Subscription parent = Operators.emptySubscription();
		up.onSubscribe(parent);

		FluxUsingWhen.RollbackInner op = new FluxUsingWhen.RollbackInner(up, new IllegalStateException("rollback cause"));

		assertThat(op.scan(Attr.PARENT)).as("PARENT").isSameAs(up);
		assertThat(op.scan(Attr.ACTUAL)).as("ACTUAL").isSameAs(up.actual);
		assertThat(op.scan(Attr.RUN_STYLE)).isSameAs(Attr.RunStyle.SYNC);

		assertThat(op.scan(Attr.TERMINATED)).as("TERMINATED before").isFalse();

		op.onComplete();
		assertThat(op.scan(Attr.TERMINATED))
				.as("TERMINATED by complete")
				.isSameAs(up.scan(Attr.TERMINATED))
				.isTrue();
		assertThat(up.scan(Attr.ERROR)).as("parent ERROR").hasMessage("rollback cause");

		assertThat(op.scanUnsafe(Attr.PREFETCH)).as("PREFETCH not supported").isNull();
	}

	@Test
	public void scanCancelInner() {
		CoreSubscriber<? super Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		UsingWhenSubscriber<Integer, String> up = new UsingWhenSubscriber<>(actual, "RESOURCE", Mono::just, (s, err) -> Mono.just(s), Mono::just, null);
		final Subscription parent = Operators.emptySubscription();
		up.onSubscribe(parent);

		FluxUsingWhen.CancelInner op = new FluxUsingWhen.CancelInner(up);

		assertThat(op.scan(Attr.PARENT)).as("PARENT").isSameAs(up);
		assertThat(op.scan(Attr.ACTUAL)).as("ACTUAL").isSameAs(up.actual);
		assertThat(op.scanUnsafe(Attr.PREFETCH)).as("PREFETCH not supported").isNull();
		assertThat(op.scan(Attr.RUN_STYLE)).isSameAs(Attr.RunStyle.SYNC);
	}

	// == utility test classes ==
	static class TestResource {

		private static final Duration DELAY = Duration.ofMillis(100);

		final Level level;

		PublisherProbe<Integer> commitProbe = PublisherProbe.empty();
		PublisherProbe<Integer> rollbackProbe = PublisherProbe.empty();
		PublisherProbe<Integer> cancelProbe = PublisherProbe.empty();

		TestResource() {
			this.level = Level.FINE;
		}

		TestResource(Level level) {
			this.level = level;
		}

		public Flux<String> data() {
			return Flux.just("Transaction started");
		}

		public Flux<Integer> commit() {
			this.commitProbe = PublisherProbe.of(
					Flux.just(3, 2, 1)
					    .log("commit method used", level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return commitProbe.flux();
		}

		public Flux<Integer> commitDelay() {
			this.commitProbe = PublisherProbe.of(
					Flux.just(3, 2, 1)
					    .delayElements(DELAY)
					    .log("commit method used", level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return commitProbe.flux();
		}

		public Flux<Integer> commitError() {
			this.commitProbe = PublisherProbe.of(
					Flux.just(3, 2, 1)
					    .delayElements(DELAY)
					    .map(i -> 100 / (i - 1)) //results in divide by 0
					    .log("commit method used", level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return commitProbe.flux();
		}

		@Nullable
		public Flux<Integer> commitNull() {
			return null;
		}

		public Flux<Integer> rollback(Throwable error) {
			this.rollbackProbe = PublisherProbe.of(
					Flux.just(5, 4, 3, 2, 1)
					    .log("rollback me thod used on: " + error, level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return rollbackProbe.flux();
		}

		public Flux<Integer> rollbackDelay(Throwable error) {
			this.rollbackProbe = PublisherProbe.of(
					Flux.just(5, 4, 3, 2, 1)
					    .delayElements(DELAY)
					    .log("rollback method used on: " + error, level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return rollbackProbe.flux();
		}

		public Flux<Integer> rollbackError(Throwable error) {
			this.rollbackProbe = PublisherProbe.of(
					Flux.just(5, 4, 3, 2, 1)
					    .delayElements(DELAY)
					    .map(i -> 100 / (i - 1)) //results in divide by 0
					    .log("rollback method used on: " + error, level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return rollbackProbe.flux();
		}

		@Nullable
		public Flux<Integer> rollbackNull(Throwable error) {
			return null;
		}

		public Flux<Integer> cancel() {
			this.cancelProbe = PublisherProbe.of(
					Flux.just(5, 4, 3, 2, 1)
					    .log("cancel method used", level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return cancelProbe.flux();
		}

		public Flux<Integer> cancelError() {
			this.cancelProbe = PublisherProbe.of(
					Flux.just(5, 4, 3, 2, 1)
					    .delayElements(DELAY)
					    .map(i -> 100 / (i - 1)) //results in divide by 0
					    .log("cancel method used", level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return cancelProbe.flux();
		}

		@Nullable
		public Flux<Integer> cancelNull() {
			return null;
		}
	}

	//unit test parameter providers

	private static Object[] sources01() {
		return new Object[] {
				new Object[] { Flux.interval(Duration.ofMillis(100)).map(String::valueOf) },
				new Object[] { Flux.range(0, 2).map(String::valueOf) }
		};
	}

	private static Object[] sourcesFullTransaction() {
		return new Object[] {
				new Object[] { Flux.just("Transaction started", "work in transaction", "more work in transaction").hide() },
				new Object[] { Flux.just("Transaction started", "work in transaction", "more work in transaction") }
		};
	}

	private static Object[] sourcesTransactionError() {
		return new Object[] {
				new Object[] { Flux.just("Transaction started", "work in transaction")
						.concatWith(Mono.error(new IllegalStateException("boom"))) },
				new Object[] { Flux.just("Transaction started", "work in transaction", "boom")
						.map(v -> { if (v.length() > 4) return v; else throw new IllegalStateException("boom"); } ) }
		};
	}

	private static Object[] sourcesContext() {
		return new Object[] {
				new Object[] { Mono.deferContextual(Mono::just).map(it -> it.get(String.class)).hide() },
				new Object[] { Mono.deferContextual(Mono::just).map(it -> it.get(String.class)) }
		};
	}

	private static Object[] sourcesContextError() {
		return new Object[] {
				new Object[] { Mono
						.deferContextual(Mono::just)
						.map(it -> it.get(String.class))
						.hide()
						.map(it -> { throw new IllegalStateException("boom"); })
				},
				new Object[] { Mono
						.deferContextual(Mono::just)
						.map(it -> it.get(String.class))
						.map(it -> { throw new IllegalStateException("boom"); })
				}
		};
	}

}
