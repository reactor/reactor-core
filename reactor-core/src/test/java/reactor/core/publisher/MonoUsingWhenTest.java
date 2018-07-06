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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
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

		TestPublisher<String> testPublisher = TestPublisher.createCold();
		testPublisher.next("Resource").error(new IllegalStateException("boom"));

		Mono<String> test = Mono.usingWhen(testPublisher,
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

		TestPublisher<String> testPublisher = TestPublisher.createCold();
		testPublisher.emit("Resource", "boom");

		Mono<String> test = Mono.usingWhen(testPublisher,
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
				tr -> Mono.fromRunnable(() -> rollbackDone.set(true)),
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
				tr -> Mono.fromRunnable(() -> rollbackDone.set(true)),
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
				Flux::just,
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
				FluxUsingWhenTest.TestResource::rollback);

		StepVerifier.create(test)
		            .verifyErrorSatisfies(e -> assertThat(e).hasMessage("boom"));

		testResource.commitProbe.assertWasNotSubscribed();
		testResource.rollbackProbe.assertWasSubscribed();
	}

	@Test
	public void nullClosureAppliesRollback() {
		FluxUsingWhenTest.TestResource testResource = new FluxUsingWhenTest.TestResource();

		Mono<String> test = Mono.usingWhen(Mono.just(testResource),
				tr -> null,
				FluxUsingWhenTest.TestResource::commit,
				FluxUsingWhenTest.TestResource::rollback);

		StepVerifier.create(test)
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(NullPointerException.class)
				            .hasMessage("The resourceClosure function returned a null value"));

		testResource.commitProbe.assertWasNotSubscribed();
		testResource.rollbackProbe.assertWasSubscribed();
	}

	@Test
	public void resourceSupplierCanAccessContext() {
		Mono.usingWhen(Mono.subscriberContext()
		                   .map(ctx -> ctx.get(String.class)),
				Mono::just,
				Mono::just,
				Mono::just,
				Mono::just)
		    .subscriberContext(Context.of(String.class, "contextual"))
		    .as(StepVerifier::create)
		    .expectNext("contextual")
		    .verifyComplete();
	}

	// == scanUnsafe tests ==

	@Test
	public void scanOperator() {
		MonoUsingWhen<Object, Object> op = new MonoUsingWhen<>(Mono.empty(), Mono::just, Mono::just, Mono::just, Mono::just);

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
		ResourceSubscriber<String, Integer> op = new ResourceSubscriber<>(actual, s -> Mono.just(s.length()), Mono::just, Mono::just, Mono::just, true);
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

}