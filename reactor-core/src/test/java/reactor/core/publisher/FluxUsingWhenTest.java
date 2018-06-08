/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.Scannable.Attr;
import reactor.core.ScannableTest;
import reactor.core.publisher.FluxUsingWhen.ResourceSubscriber;
import reactor.core.publisher.FluxUsingWhen.UsingWhenFuseableSubscriber;
import reactor.core.publisher.FluxUsingWhen.UsingWhenSubscriber;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.TestLogger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import static org.assertj.core.api.Assertions.*;

@RunWith(JUnitParamsRunner.class)
public class FluxUsingWhenTest {

	@Test
	public void nullResourcePublisherRejected() {
		assertThatNullPointerException()
				.isThrownBy(() -> Flux.usingWhen(null,
						tr -> Mono.empty(),
						tr -> Mono.empty(),
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

		TestPublisher<String> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		testPublisher.replayOnSubscribe(tp -> tp.next("Resource").error(new IllegalStateException("boom")));

		Flux<String> test = Flux.usingWhen(testPublisher,
				Mono::just,
				tr -> Mono.fromRunnable(() -> commitDone.set(true)),
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

		TestPublisher<String> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		testPublisher.replayOnSubscribe(tp -> tp.emit("Resource", "boom"));

		Flux<String> test = Flux.usingWhen(testPublisher,
				Mono::just,
				tr -> Mono.fromRunnable(() -> commitDone.set(true)),
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

		Mono<String> test = Mono.usingWhen(resourcePublisher,
				Mono::just,
				tr -> Mono.fromRunnable(() -> commitDone.set(true)),
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
	public void failToGenerateClosureAppliesRollback() {
		TestResource testResource = new TestResource();

		Flux<String> test = Flux.usingWhen(Mono.just(testResource),
				tr -> {
					throw new UnsupportedOperationException("boom");
				},
				TestResource::commit,
				TestResource::rollback);

		StepVerifier.create(test)
		            .verifyErrorSatisfies(e -> assertThat(e).hasMessage("boom"));

		testResource.commitProbe.assertWasNotSubscribed();
		testResource.rollbackProbe.assertWasSubscribed();
	}

	@Test
	public void nullClosureAppliesRollback() {
		TestResource testResource = new TestResource();

		Flux<String> test = Flux.usingWhen(Mono.just(testResource),
				tr -> null,
				TestResource::commit,
				TestResource::rollback);

		StepVerifier.create(test)
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(NullPointerException.class)
				            .hasMessage("The resourceClosure function returned a null value"));

		testResource.commitProbe.assertWasNotSubscribed();
		testResource.rollbackProbe.assertWasSubscribed();
	}

	@Test
	@Parameters(method = "sources01")
	public void cancelWithHandler(Flux<String> source) {
		TestResource testResource = new TestResource();

		Flux<String> test = Flux.usingWhen(Mono.just(testResource),
				tr -> source,
				TestResource::commit,
				TestResource::rollback,
				TestResource::rollback)
		                        .take(2);

		StepVerifier.create(test)
		            .expectNext("0", "1")
		            .verifyComplete();

		testResource.commitProbe.assertWasNotSubscribed();
		testResource.rollbackProbe.assertWasSubscribed();
	}

	@Test
	@Parameters(method = "sources01")
	public void cancelWithHandlerFailure(Flux<String> source) {
		TestResource testResource = new TestResource();
		final TestLogger tl = new TestLogger();
		Loggers.useCustomLoggers(name -> tl);

		try {
			Flux<String> test = Flux.usingWhen(Mono.just(testResource),
					tr -> source,
					TestResource::commit,
					TestResource::rollback,
					r -> r.rollback()
					      //immediate error to trigger the logging within the test
					      .concatWith(Mono.error(new IllegalStateException("rollback error")))
			)
			                        .take(2);

			StepVerifier.create(test)
			            .expectNext("0", "1")
			            .verifyComplete();

			testResource.commitProbe.assertWasNotSubscribed();
			testResource.rollbackProbe.assertWasSubscribed();
		}
		finally {
			Loggers.resetLoggerFactory();
		}
		assertThat(tl.getErrContent())
				.contains("Async resource cleanup failed after cancel")
				.contains("java.lang.IllegalStateException: rollback error");
	}

	@Test
	@Parameters(method = "sources01")
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
			testResource.rollbackProbe.assertWasNotSubscribed();
		}
		finally {
			Loggers.resetLoggerFactory();
		}
		assertThat(tl.getErrContent())
				.contains("Error generating async resource cleanup during onCancel")
				.contains("java.lang.NullPointerException");
	}

	@Test
	@Parameters(method = "sources01")
	public void cancelWithoutHandlerAppliesCommit(Flux<String> source) {
		TestResource testResource = new TestResource();

		Flux<String> test = Flux
				.usingWhen(Mono.just(testResource),
						tr -> source,
						TestResource::commit,
						TestResource::rollback)
				.take(2);

		StepVerifier.create(test)
		            .expectNext("0", "1")
		            .verifyComplete();

		testResource.commitProbe.assertWasSubscribed();
		testResource.rollbackProbe.assertWasNotSubscribed();
	}

	@Test
	@Parameters(method = "sources01")
	public void cancelDefaultHandlerFailure(Flux<String> source) {
		TestResource testResource = new TestResource();
		final TestLogger tl = new TestLogger();
		Loggers.useCustomLoggers(name -> tl);

		try {
			Flux<String> test = Flux.usingWhen(Mono.just(testResource),
					tr -> source,
					r -> r.commit()
					      //immediate error to trigger the logging within the test
					      .concatWith(Mono.error(new IllegalStateException("commit error"))),
					TestResource::rollback
			)
			                        .take(2);

			StepVerifier.create(test)
			            .expectNext("0", "1")
			            .verifyComplete();

			testResource.commitProbe.assertWasSubscribed();
			testResource.rollbackProbe.assertWasNotSubscribed();
		}
		finally {
			Loggers.resetLoggerFactory();
		}
		assertThat(tl.getErrContent())
				.contains("Async resource cleanup failed after cancel")
				.contains("java.lang.IllegalStateException: commit error");
	}

	@Test
	@Parameters(method = "sourcesFullTransaction")
	public void apiCommit(Flux<String> fullTransaction) {
		final AtomicReference<TestResource> ref = new AtomicReference<>();

		Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
				d -> {
					ref.set(d);
					return fullTransaction;
				},
				TestResource::commit,
				TestResource::rollback);

		StepVerifier.create(flux)
		            .expectNext("Transaction started")
		            .expectNext("work in transaction")
		            .expectNext("more work in transaction")
		            .expectComplete()
		            .verify();

		assertThat(ref.get())
				.isNotNull()
				.matches(tr -> tr.commitProbe.wasSubscribed(), "commit method used")
				.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "no rollback");
	}

	@Test
	@Parameters(method = "sourcesFullTransaction")
	public void apiCommitFailure(Flux<String> fullTransaction) {
		final AtomicReference<TestResource> ref = new AtomicReference<>();

		Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
				d -> {
					ref.set(d);
					return fullTransaction;
				},
				TestResource::commitError,
				TestResource::rollback);

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
				.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "no rollback");
	}

	@Test
	@Parameters(method = "sourcesFullTransaction")
	public void commitGeneratingNull(Flux<String> fullTransaction) {
		final AtomicReference<TestResource> ref = new AtomicReference<>();

		Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
				d -> {
					ref.set(d);
					return fullTransaction;
				},
				TestResource::commitNull,
				TestResource::rollback);

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
				.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "no rollback");
	}

	@Test
	@Parameters(method = "sourcesTransactionError")
	public void apiRollback(Flux<String> transactionWithError) {
		final AtomicReference<TestResource> ref = new AtomicReference<>();
		Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
				d -> {
					ref.set(d);
					return transactionWithError;
				},
				TestResource::commitError,
				TestResource::rollback);

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
				.matches(tr -> tr.rollbackProbe.wasSubscribed(), "rollback method used");
	}

	@Test
	@Parameters(method = "sourcesTransactionError")
	public void apiRollbackFailure(Flux<String> transactionWithError) {
		final AtomicReference<TestResource> ref = new AtomicReference<>();
		Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
				d -> {
					ref.set(d);
					return transactionWithError;
				},
				TestResource::commitError,
				TestResource::rollbackError);

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
				.matches(tr -> tr.rollbackProbe.wasSubscribed(), "rollback method used");
	}

	@Test
	@Parameters(method = "sourcesTransactionError")
	public void apiRollbackGeneratingNull(Flux<String> transactionWithError) {
		final AtomicReference<TestResource> ref = new AtomicReference<>();
		Flux<String> flux = Flux.usingWhen(Mono.fromCallable(TestResource::new),
				d -> {
					ref.set(d);
					return transactionWithError;
				},
				TestResource::commitError,
				TestResource::rollbackNull);

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
				.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "rollback method short-circuited");
	}

	@Test
	public void apiAsyncCleanup() {
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
				.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "no rollback");
	}

	@Test
	public void apiAsyncCleanupFailure() {
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
				.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "no rollback");
	}

	@Test
	public void normalHasNoOpQueueOperations() {
		final FluxPeekFuseableTest.AssertQueueSubscription<String> assertQueueSubscription =
				new FluxPeekFuseableTest.AssertQueueSubscription<>();
		assertQueueSubscription.offer("foo");

		UsingWhenSubscriber<String, String>
				test = new UsingWhenSubscriber<>(
				new LambdaSubscriber<>(null, null, null, null),
				"resource", it -> Mono.empty(), it -> Mono.empty(), null);

		test.onSubscribe(assertQueueSubscription);

		assertThat(test).isInstanceOf(Fuseable.QueueSubscription.class);
		assertThat(test.isEmpty()).as("isEmpty").isTrue();
		assertThat(test.size()).as("size").isZero();
		assertThat(test.requestFusion(Fuseable.ANY)).as("requestFusion(ANY)").isZero();
		assertThat(test.poll()).as("poll").isNull();
		assertThatCode(test::clear).doesNotThrowAnyException();
	}

	@Test
	public void fuseableQueueOperations() {
		final FluxPeekFuseableTest.AssertQueueSubscription<String> assertQueueSubscription =
				new FluxPeekFuseableTest.AssertQueueSubscription<>();
		assertQueueSubscription.offer("foo");

		UsingWhenFuseableSubscriber<String, String>
				test = new UsingWhenFuseableSubscriber<>(
				new LambdaSubscriber<>(null, null, null, null),
				"resource", it -> Mono.empty(), it -> Mono.empty(), null);

		test.onSubscribe(assertQueueSubscription);

		assertThat(test).isInstanceOf(Fuseable.QueueSubscription.class);
		assertThat(test.isEmpty()).as("isEmpty").isFalse();
		assertThat(test.size()).as("size").isOne();
		assertThat(test.requestFusion(Fuseable.ASYNC)).as("requestFusion(ASYNC)").isEqualTo(Fuseable.ASYNC);
		assertThat(test.requestFusion(Fuseable.SYNC)).as("requestFusion(SYNC)").isEqualTo(Fuseable.SYNC);
		assertThat(test.poll()).as("poll #1").isEqualTo("foo");
		assertThat(test.poll()).as("poll #2").isNull();

		assertQueueSubscription.offer("bar");
		assertThat(assertQueueSubscription.size()).as("before clear").isOne();
		test.clear();
		assertThat(assertQueueSubscription.size()).as("after clear").isZero();
	}

	@Test
	public void syncFusionPollRollbackErrorLogs() {
		TestLogger testLogger = new TestLogger();
		Loggers.useCustomLoggers(it -> testLogger);

		final FluxPeekFuseableTest.AssertQueueSubscription<String> assertQueueSubscription =
				new FluxPeekFuseableTest.AssertQueueSubscription<>();
		assertQueueSubscription.offer("foo");

		UsingWhenFuseableSubscriber<String, String>
				test = new UsingWhenFuseableSubscriber<>(
				new LambdaSubscriber<>(null, null, null, null),
				"resource", it -> Mono.error(new IllegalStateException("asyncComplete error")), it -> Mono.empty(), null);

		try {
			test.onSubscribe(assertQueueSubscription);
			test.requestFusion(Fuseable.SYNC);
			assertThat(test.poll()).as("poll #1").isEqualTo("foo");
			assertThat(test.poll()).as("poll #2").isNull();
		}
		finally {
			Loggers.resetLoggerFactory();
		}

		assertThat(testLogger.getErrContent())
				.contains("Async resource cleanup failed after poll")
				.contains("java.lang.IllegalStateException: asyncComplete error");
	}

	// == scanUnsafe tests ==

	@Test
	public void scanOperator() {
		FluxUsingWhen<Object, Object> op = new FluxUsingWhen<>(Mono.empty(), Mono::just, Mono::just, Mono::just, Mono::just);

		assertThat(op.scanUnsafe(Attr.ACTUAL))
				.isSameAs(op.scanUnsafe(Attr.ACTUAL_METADATA))
				.isSameAs(op.scanUnsafe(Attr.BUFFERED))
				.isSameAs(op.scanUnsafe(Attr.CAPACITY))
				.isSameAs(op.scanUnsafe(Attr.CANCELLED))
				.isSameAs(op.scanUnsafe(Attr.DELAY_ERROR))
				.isSameAs(op.scanUnsafe(Attr.ERROR))
				.isSameAs(op.scanUnsafe(Attr.LARGE_BUFFERED))
				.isSameAs(op.scanUnsafe(Attr.NAME))
				.isSameAs(op.scanUnsafe(Attr.PARENT))
				.isSameAs(op.scanUnsafe(Attr.RUN_ON))
				.isSameAs(op.scanUnsafe(Attr.PREFETCH))
				.isSameAs(op.scanUnsafe(Attr.REQUESTED_FROM_DOWNSTREAM))
				.isSameAs(op.scanUnsafe(Attr.TERMINATED))
				.isSameAs(op.scanUnsafe(Attr.TAGS))
				.isNull();
	}

	@Test
	public void scanResourceSubscriber() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		ResourceSubscriber<String, Integer> op = new ResourceSubscriber<>(actual, s -> Flux.just(s.length()), Mono::just, Mono::just, Mono::just, true);
		final Subscription parent = Operators.emptySubscription();
		op.onSubscribe(parent);

		assertThat(op.scan(Attr.PARENT)).as("PARENT").isSameAs(parent);
		assertThat(op.scan(Attr.ACTUAL)).as("ACTUAL").isSameAs(actual);

		assertThat(op.scan(Attr.PREFETCH)).as("PREFETCH").isEqualTo(Integer.MAX_VALUE);

		assertThat(op.scan(Attr.TERMINATED)).as("TERMINATED").isFalse();
		op.resourceProvided = true;
		assertThat(op.scan(Attr.TERMINATED)).as("TERMINATED resourceProvided").isTrue();

		assertThat(op.scanUnsafe(Attr.CANCELLED)).as("CANCELLED not supported").isNull();
	}

	@Test
	public void scanUsingWhenSubscriber() {
		CoreSubscriber<? super Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		UsingWhenSubscriber<Integer, String> op = new UsingWhenSubscriber<>(actual, "RESOURCE", Mono::just, Mono::just, Mono::just);
		final Subscription parent = Operators.emptySubscription();
		op.onSubscribe(parent);

		assertThat(op.scan(Attr.PARENT)).as("PARENT").isSameAs(parent);
		assertThat(op.scan(Attr.ACTUAL)).as("ACTUAL")
		                                .isSameAs(actual)
		                                .isSameAs(op.actual());

		assertThat(op.scan(Attr.TERMINATED)).as("pre TERMINATED").isFalse();
		assertThat(op.scan(Attr.CANCELLED)).as("pre CANCELLED").isFalse();

		op.deferredError(new IllegalStateException("boom"));
		assertThat(op.scan(Attr.TERMINATED)).as("TERMINATED with error").isTrue();
		assertThat(op.scan(Attr.ERROR)).as("ERROR").hasMessage("boom");

		op.cancel();
		assertThat(op.scan(Attr.CANCELLED)).as("CANCELLED").isTrue();
	}

	@Test
	public void scanUsingWhenFuseableSubscriber() {
		CoreSubscriber<? super Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		UsingWhenFuseableSubscriber<Integer, String> op = new UsingWhenFuseableSubscriber<>(actual, "RESOURCE", Mono::just, Mono::just, Mono::just);
		final Subscription parent = Operators.emptySubscription();
		op.onSubscribe(parent);

		assertThat(op.scan(Attr.PARENT)).as("PARENT").isSameAs(parent);
		assertThat(op.scan(Attr.ACTUAL)).as("ACTUAL")
		                                .isSameAs(actual)
		                                .isSameAs(op.actual());

		assertThat(op.scan(Attr.TERMINATED)).as("pre TERMINATED").isFalse();

		op.deferredError(new IllegalStateException("boom"));
		assertThat(op.scan(Attr.TERMINATED)).as("TERMINATED with error").isTrue();
		assertThat(op.scan(Attr.ERROR)).as("ERROR").hasMessage("boom");

		//need something different from EmptySubscription to detect cancel
		op.qs = null;
		assertThat(op.scan(Attr.CANCELLED)).as("pre CANCELLED").isFalse();
		op.cancel();
		assertThat(op.scan(Attr.CANCELLED)).as("CANCELLED").isTrue();
	}

	@Test
	public void scanCommitInner() {
		CoreSubscriber<? super Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		UsingWhenSubscriber<Integer, String> up = new UsingWhenSubscriber<>(actual, "RESOURCE", Mono::just, Mono::just, Mono::just);
		final Subscription parent = Operators.emptySubscription();
		up.onSubscribe(parent);

		FluxUsingWhen.CommitInner op = new FluxUsingWhen.CommitInner(up);

		assertThat(op.scan(Attr.PARENT)).as("PARENT").isSameAs(up);
		assertThat(op.scan(Attr.ACTUAL)).as("ACTUAL").isSameAs(up.actual);

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
	public void scanRollbackSubscriber() {
		CoreSubscriber<? super Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		UsingWhenSubscriber<Integer, String> up = new UsingWhenSubscriber<>(actual, "RESOURCE", Mono::just, Mono::just, Mono::just);
		final Subscription parent = Operators.emptySubscription();
		up.onSubscribe(parent);

		FluxUsingWhen.RollbackInner op = new FluxUsingWhen.RollbackInner(up, new IllegalStateException("rollback cause"));

		assertThat(op.scan(Attr.PARENT)).as("PARENT").isSameAs(up);
		assertThat(op.scan(Attr.ACTUAL)).as("ACTUAL").isSameAs(up.actual);

		assertThat(op.scan(Attr.TERMINATED)).as("TERMINATED before").isFalse();

		op.onComplete();
		assertThat(op.scan(Attr.TERMINATED))
				.as("TERMINATED by complete")
				.isSameAs(up.scan(Attr.TERMINATED))
				.isTrue();
		assertThat(up.scan(Attr.ERROR)).as("parent ERROR").hasMessage("rollback cause");

		assertThat(op.scanUnsafe(Attr.PREFETCH)).as("PREFETCH not supported").isNull();
	}

	// == utility test classes ==
	static class TestResource {

		private static final Duration DELAY = Duration.ofMillis(100);

		final Level level;

		PublisherProbe<Integer> commitProbe = PublisherProbe.empty();
		PublisherProbe<Integer> rollbackProbe = PublisherProbe.empty();

		TestResource() {
			this.level = Level.INFO;
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

		public Flux<Integer> rollback() {
			this.rollbackProbe = PublisherProbe.of(
					Flux.just(5, 4, 3, 2, 1)
					    .log("rollback method used", level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return rollbackProbe.flux();
		}

		public Flux<Integer> rollbackDelay() {
			this.rollbackProbe = PublisherProbe.of(
					Flux.just(5, 4, 3, 2, 1)
					    .delayElements(DELAY)
					    .log("rollback method used", level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return rollbackProbe.flux();
		}

		public Flux<Integer> rollbackError() {
			this.rollbackProbe = PublisherProbe.of(
					Flux.just(5, 4, 3, 2, 1)
					    .delayElements(DELAY)
					    .map(i -> 100 / (i - 1)) //results in divide by 0
					    .log("rollback method used", level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return rollbackProbe.flux();
		}

		@Nullable
		public Flux<Integer> rollbackNull() {
			return null;
		}
	}

	//unit test parameter providers

	private Object[] sources01() {
		return new Object[] {
				new Object[] { Flux.interval(Duration.ofMillis(100)).map(String::valueOf) },
				new Object[] { Flux.range(0, 2).map(String::valueOf) }
		};
	}

	private Object[] sourcesFullTransaction() {
		return new Object[] {
				new Object[] { Flux.just("Transaction started", "work in transaction", "more work in transaction").hide() },
				new Object[] { Flux.just("Transaction started", "work in transaction", "more work in transaction") }
		};
	}

	private Object[] sourcesTransactionError() {
		return new Object[] {
				new Object[] { Flux.just("Transaction started", "work in transaction")
						.concatWith(Mono.error(new IllegalStateException("boom"))) },
				new Object[] { Flux.just("Transaction started", "work in transaction", "boom")
						.map(v -> { if (v.length() > 4) return v; else throw new IllegalStateException("boom"); } ) }
		};
	}


}