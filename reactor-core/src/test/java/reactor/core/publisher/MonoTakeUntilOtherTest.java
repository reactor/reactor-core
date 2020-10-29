/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoTakeUntilOtherTest {

	@Test
	public void nullSource() {
		//noinspection ConstantConditions
		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> new MonoTakeUntilOther<>(null, Flux.never()))
				.withMessage(null);
	}

	@Test
	public void nullOther() {
		//noinspection ConstantConditions
		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> new MonoTakeUntilOther<>(Mono.just("foo"), null))
				.withMessage("other");
	}

	@Test
	public void neverSourceIsCancelled() {
		AtomicReference<SignalType> signal = new AtomicReference<>();

		StepVerifier.withVirtualTime(() ->
				new MonoTakeUntilOther<>(Mono.never().doFinally(signal::set), Mono.delay(Duration.ofMillis(100)))
		)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .verifyComplete();

		assertThat(signal).hasValue(SignalType.CANCEL);
	}

	@Test
	public void disposeCancelsBoth() {
		AtomicReference<SignalType> s1 = new AtomicReference<>();
		AtomicReference<SignalType> s2 = new AtomicReference<>();

		StepVerifier.create(new MonoTakeUntilOther<>(Mono.never().doFinally(s1::set),
				Mono.never().doFinally(s2::set)))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenCancel()
		            .verify(Duration.ofMillis(500));

		assertThat(s1).hasValue(SignalType.CANCEL);
		assertThat(s2).hasValue(SignalType.CANCEL);
	}

	@Test
	public void apiTakeShortcircuits() {
		StepVerifier.withVirtualTime(() ->
				Mono.delay(Duration.ofMillis(200))
				    .take(Duration.ofMillis(100))
		)
		            .thenAwait(Duration.ofMillis(300))
		            .verifyComplete();
	}

	@Test
	public void apiTakeSchedulerShortcircuits() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		StepVerifier.create(
				Mono.delay(Duration.ofMillis(200))
				    .take(Duration.ofSeconds(10), vts)
		)
		            .then(() -> vts.advanceTimeBy(Duration.ofSeconds(10)))
		            .verifyComplete();
	}

	@Test
	public void apiTakeValuedBeforeDuration() {
		StepVerifier.withVirtualTime(() ->
				Mono.delay(Duration.ofMillis(100))
				    .take(Duration.ofMillis(200))
		)
		            .thenAwait(Duration.ofMillis(200))
		            .expectNext(0L)
		            .verifyComplete();
	}

	@Test
	public void apiTakeErrorBeforeDuration() {
		StepVerifier.withVirtualTime(() ->
				Mono.delay(Duration.ofMillis(100))
				    .then(Mono.error(new IllegalStateException("boom")))
				    .take(Duration.ofMillis(200))
		)
		            .thenAwait(Duration.ofMillis(200))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void apiTakeCompleteBeforeDuration() {
		StepVerifier.withVirtualTime(() ->
				Mono.delay(Duration.ofMillis(100))
				    .ignoreElement()
				    .take(Duration.ofMillis(200))
		)
		            .thenAwait(Duration.ofMillis(200))
		            .verifyComplete();
	}

	@Test
	public void apiTakeUntilOtherShortcircuits() {
		TestPublisher<String> other = TestPublisher.create();

		StepVerifier.withVirtualTime(() ->
				Mono.delay(Duration.ofMillis(200))
				    .takeUntilOther(other)
		)
		            .thenAwait(Duration.ofMillis(100))
		            .then(() -> other.next("go"))
		            .verifyComplete();

		other.assertCancelled();
	}

	@Test
	public void apiTakeUntilOtherValuedBeforeOther() {
		TestPublisher<String> other = TestPublisher.create();

		StepVerifier.withVirtualTime(() ->
				Mono.delay(Duration.ofMillis(100))
				    .takeUntilOther(other)
		)
		            .thenAwait(Duration.ofMillis(200))
		            .then(() -> other.next("go"))
		            .expectNext(0L)
		            .verifyComplete();

		other.assertCancelled();
	}

	@Test
	public void apiTakeUntilOtherErrorBeforeOther() {
		TestPublisher<String> other = TestPublisher.create();

		StepVerifier.withVirtualTime(() ->
				Mono.delay(Duration.ofMillis(100))
				    .then(Mono.error(new IllegalStateException("boom")))
				    .takeUntilOther(other)
		)
		            .thenAwait(Duration.ofMillis(200))
		            .then(() -> other.next("go"))
		            .verifyErrorMessage("boom");

		other.assertCancelled();
	}

	@Test
	public void apiTakeUntilOtherCompleteBeforeOther() {
		TestPublisher<String> other = TestPublisher.create();

		StepVerifier.withVirtualTime(() ->
				Mono.delay(Duration.ofMillis(100))
				    .ignoreElement()
				    .takeUntilOther(other)
		)
		            .thenAwait(Duration.ofMillis(200))
		            .then(() -> other.next("go"))
		            .verifyComplete();

		other.assertCancelled();
	}

	@Test
	public void scanOperator(){
		TestPublisher<String> other = TestPublisher.create();
	    MonoTakeUntilOther<Integer, String> test = new MonoTakeUntilOther<>(Mono.just(1), other);

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
