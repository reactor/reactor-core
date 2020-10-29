/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class
MonoCompletionStageTest {

	@Test
	public void cancelThenFutureFails() {
		CompletableFuture<Integer> future = new CompletableFuture<>();
		AtomicReference<Subscription> subRef = new AtomicReference<>();

		Mono<Integer> mono = Mono
				.fromFuture(future)
				.doOnSubscribe(subRef::set);

		StepVerifier.create(mono)
		            .expectSubscription()
		            .then(() -> {
		            	subRef.get().cancel();
		            	future.completeExceptionally(new IllegalStateException("boom"));
		            	future.complete(1);
		            })
		            .thenCancel()//already cancelled but need to get to verification
		            .verifyThenAssertThat()
		            .hasDroppedErrorWithMessage("boom");
	}

	@Test
	public void cancelFutureImmediatelyCancelledLoop() {
		for (int i = 0; i < 10000; i++) {
			CompletableFuture<Integer> future = new CompletableFuture<>();
			Mono<Integer> mono = Mono
					.fromFuture(future)
					.doFinally(sig -> {
						if (sig == SignalType.CANCEL) future.cancel(false);
					});

			StepVerifier.create(mono)
			            .expectSubscription()
			            .thenCancel()
			            .verifyThenAssertThat()
			            .hasNotDroppedErrors();

			assertThat(future).isCancelled();
		}
	}

	@Test
	public void cancelFutureDelayedCancelledLoop() {
		for (int i = 0; i < 500; i++) {
			CompletableFuture<Integer> future = new CompletableFuture<>();
			Mono<Integer> mono = Mono
					.fromFuture(future)
					.doFinally(sig -> {
						if (sig == SignalType.CANCEL) future.cancel(false);
					});

			StepVerifier.create(mono)
			            .expectSubscription()
			            .thenAwait(Duration.ofMillis(10))
			            .thenCancel()
			            .verifyThenAssertThat()
			            .hasNotDroppedErrors();

			assertThat(future).isCancelled();
		}
	}

	@Test
	public void cancelFutureTimeoutCancelledLoop() {
		for (int i = 0; i < 500; i++) {
			CompletableFuture<Integer> future = new CompletableFuture<>();
			Mono<Integer> mono = Mono
					.fromFuture(future)
					.doFinally(sig -> {
						if (sig == SignalType.CANCEL) future.cancel(false);
					});

			StepVerifier.create(mono.timeout(Duration.ofMillis(10)))
			            .expectSubscription()
			            .expectErrorSatisfies(e ->
					            assertThat(e).hasMessageStartingWith("Did not observe any item or terminal signal within 10ms"))
			            .verifyThenAssertThat()
			            .hasNotDroppedErrors();

			assertThat(future).isCancelled();
		}
	}

	@Test
	public void fromCompletableFuture(){
		CompletableFuture<String> f = CompletableFuture.supplyAsync(() -> "helloFuture");

		assertThat(Mono.fromFuture(f)
		               .block()).isEqualToIgnoringCase("helloFuture");
	}

	@Test
	public void fromCompletionStage() {
		CompletionStage<String> completionStage = CompletableFuture.supplyAsync(() -> "helloFuture");

		assertThat(Mono.fromCompletionStage(completionStage).block())
				.isEqualTo("helloFuture");
	}

	@Test
	public void stackOverflowGoesToOnErrorDropped() {
		CompletableFuture<Integer> future = new CompletableFuture<>();
		future.complete(1);
		Mono<Integer> simple = Mono.fromFuture(future);

		StepVerifier.create(
				simple.map(r -> {
					throw new StackOverflowError("boom, good bye Future");
				})
		)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(1))
		            .thenCancel()
		            .verifyThenAssertThat()
		            .hasDroppedErrorWithMessage("boom, good bye Future");
	}

	@Test
	public void scanOperator(){
		CompletionStage<String> completionStage = CompletableFuture.supplyAsync(() -> "helloFuture");
		MonoCompletionStage<String> test = new MonoCompletionStage<>(completionStage);

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isNull();
	}

	@Test
	public void lateFailureIsPropagatedDirectly() {
		Throwable expected = new IllegalStateException("boom");
		CompletableFuture<Integer> future = new CompletableFuture<>();

		Mono.fromCompletionStage(future)
		    .as(StepVerifier::create)
		    .then(() -> future.completeExceptionally(expected))
		    .verifyErrorSatisfies(e -> assertThat(e).isSameAs(expected));
	}

	@Test
	public void actionFailureCompletionExceptionIsUnwrapped() {
		final CompletableFuture<String> future = new CompletableFuture<>();

		Mono.fromCompletionStage(future.whenComplete((s, throwable) -> {
			throw new IllegalStateException("boom");
		}))
		    .as(StepVerifier::create)
		    .then(() -> future.complete("Success"))
		    .verifyError(IllegalStateException.class);
	}
}
