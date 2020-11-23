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

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable.Attr;
import reactor.core.publisher.MonoUsingWhen.ResourceSubscriber;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public class MonoUsingWhenTest {

	@Test
	public void nullResourcePublisherRejected() {
		assertThatNullPointerException()
				.isThrownBy(() -> Mono.usingWhen(null,
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

		Mono<String> test = Mono.usingWhen(Flux.empty().hide(),
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

		Mono<String> test = Mono.usingWhen(Flux.empty(),
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

		Mono<String> test = Mono.usingWhen(Flux.error(new IllegalStateException("boom")).hide(),
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

		Mono<String> test = Mono.usingWhen(Flux.error(new IllegalStateException("boom")),
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

		Mono<String> test = Mono.usingWhen(testPublisher,
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

		Mono<String> test = Mono.usingWhen(testPublisher,
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

		Mono<String> test = Mono.usingWhen(resourcePublisher,
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

		Mono<String> test = Mono.usingWhen(resourcePublisher,
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

		StepVerifier.create(Mono.usingWhen(resourcePublisher,
				Mono::just,
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
		Disposable disposable = Mono.usingWhen(Mono.<String>never(),
				Mono::just,
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
		FluxUsingWhenTest.TestResource testResource = new FluxUsingWhenTest.TestResource();

		Mono<String> test = Mono.usingWhen(Mono.just(testResource),
				tr -> {
					throw new UnsupportedOperationException("boom");
				},
				FluxUsingWhenTest.TestResource::commit,
				FluxUsingWhenTest.TestResource::rollback,
				FluxUsingWhenTest.TestResource::cancel);

		StepVerifier.create(test)
		            .verifyErrorSatisfies(e -> assertThat(e).hasMessage("boom"));

		testResource.commitProbe.assertWasNotSubscribed();
		testResource.cancelProbe.assertWasNotSubscribed();
		testResource.rollbackProbe.assertWasSubscribed();
	}

	@Test
	public void nullClosureAppliesRollback() {
		FluxUsingWhenTest.TestResource testResource = new FluxUsingWhenTest.TestResource();

		Mono<String> test = Mono.usingWhen(Mono.just(testResource),
				tr -> null,
				FluxUsingWhenTest.TestResource::commit,
				FluxUsingWhenTest.TestResource::rollback,
				FluxUsingWhenTest.TestResource::cancel);

		StepVerifier.create(test)
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(NullPointerException.class)
				            .hasMessage("The resourceClosure function returned a null value"));

		testResource.commitProbe.assertWasNotSubscribed();
		testResource.cancelProbe.assertWasNotSubscribed();
		testResource.rollbackProbe.assertWasSubscribed();
	}

	@Test
	public void resourceSupplierCanAccessContext() {
		Mono.usingWhen(Mono.deferContextual(Mono::just)
		                   .map(ctx -> ctx.get(String.class)),
				Mono::just,
				Mono::just,
				(res, err) -> Mono.just(res),
				Mono::just)
		    .contextWrite(Context.of(String.class, "contextual"))
		    .as(StepVerifier::create)
		    .expectNext("contextual")
		    .verifyComplete();
	}

	@Test
	public void errorCallbackReceivesCause() {
		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		NullPointerException npe = new NullPointerException("original error");

		Mono.usingWhen(Mono.just("ignored"), s -> Mono.error(npe), Mono::just,
				(res, err) -> Mono.fromRunnable(() -> errorRef.set(err)),
				Mono::just)
		    .as(StepVerifier::create)
		    .verifyErrorSatisfies(e -> assertThat(e).isSameAs(npe)
		                                            .hasNoCause()
		                                            .hasNoSuppressedExceptions());

		assertThat(errorRef).hasValue(npe);
	}

	@Test
	public void failureInApplyAsyncCompleteDiscardsValue() {
		Mono.usingWhen(Mono.just("foo"),
				resource -> Mono.just("resource " + resource),
				resource -> { throw new IllegalStateException("failure in Function"); },
				(resource, err) -> Mono.empty(),
				resource -> Mono.empty())
		    .as(StepVerifier::create)
		    .expectErrorMessage("failure in Function")
		    .verifyThenAssertThat()
		    .hasDiscarded("resource foo");
	}

	@Test
	public void onErrorAsyncCompleteDiscardsValue() {
		Mono.usingWhen(Mono.just("foo"),
				resource -> Mono.just("resource " + resource),
				resource -> Mono.error(new IllegalStateException("erroring asyncComplete")),
				(resource, err) -> Mono.empty(),
				resource -> Mono.empty())
		    .as(StepVerifier::create)
		    .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(RuntimeException.class)
		                                            .hasMessage("Async resource cleanup failed after onComplete")
		                                            .hasCause(new IllegalStateException("erroring asyncComplete")))
		    .verifyThenAssertThat()
		    .hasDiscarded("resource foo");
	}

	@Test
	public void scanOperator() {
		MonoUsingWhen<Object, Object> op = new MonoUsingWhen<>(Mono.empty(), Mono::just, Mono::just, (res, err) -> Mono.just(res), Mono::just);

		assertThat(op.scan(Attr.RUN_STYLE)).isEqualTo(Attr.RunStyle.SYNC);
	}

	@Test
	public void scanResourceSubscriber() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		ResourceSubscriber<String, Integer> op = new ResourceSubscriber<>(actual, s -> Mono.just(s.length()), Mono::just, (res, err) -> Mono.just(res), Mono::just, true);
		final Subscription parent = Operators.emptySubscription();
		op.onSubscribe(parent);

		assertThat(op.scan(Attr.PARENT)).as("PARENT").isSameAs(parent);
		assertThat(op.scan(Attr.ACTUAL)).as("ACTUAL").isSameAs(actual);

		assertThat(op.scan(Attr.PREFETCH)).as("PREFETCH").isEqualTo(Integer.MAX_VALUE);

		assertThat(op.scan(Attr.TERMINATED)).as("TERMINATED").isFalse();
		op.resourceProvided = true;
		assertThat(op.scan(Attr.TERMINATED)).as("TERMINATED resourceProvided").isTrue();

		assertThat(op.scan(Attr.RUN_STYLE)).isEqualTo(Attr.RunStyle.SYNC);

		assertThat(op.scanUnsafe(Attr.CANCELLED)).as("CANCELLED not supported").isNull();
	}

}
