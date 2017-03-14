/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoUntilOtherTest {

	@Test
	public void testMonoValuedAndPublisherVoid() {
		Publisher<Void> voidPublisher = Mono.fromRunnable(() -> { });
		StepVerifier.create(new MonoUntilOther<>(false, Mono.just("foo"), voidPublisher, Function.identity()))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void testMonoEmptyAndPublisherVoid() {
		Publisher<Void> voidPublisher = Mono.fromRunnable(() -> { });
		StepVerifier.create(new MonoUntilOther<>(false, Mono.<String>empty(), voidPublisher, Function.identity()))
		            .verifyComplete();
	}

	@Test
	public void testMonoValuedAndPublisherVoidMapped() {
		Publisher<Void> voidPublisher = Mono.fromRunnable(() -> { });
		StepVerifier.create(new MonoUntilOther<>(false, Mono.just("foo"), voidPublisher,
				String::length))
		            .expectNext(3)
		            .verifyComplete();
	}

	@Test
	public void mapperNull() {
		StepVerifier.withVirtualTime(() -> new MonoUntilOther<>(false,
				Mono.just("foo"), Mono.delay(Duration.ofMillis(500)), s -> null))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(500))
	                .verifyErrorMatches(e -> e instanceof NullPointerException &&
			                "mapper produced a null value".equals(e.getMessage()));
	}

	@Test
	public void triggerSequenceHasMultipleValuesCancelled() {
		AtomicBoolean triggerCancelled = new AtomicBoolean();
		StepVerifier.create(new MonoUntilOther<>(false,
				Mono.just("foo"),
				Flux.just(1, 2, 3).hide()
				    .delayElements(Duration.ofMillis(500))
				    .doOnCancel(() -> triggerCancelled.set(true)),
				s -> s))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(450))
		            .expectNext("foo")
		            .verifyComplete();
		assertThat(triggerCancelled.get()).isTrue();
	}

	@Test
	public void triggerSequenceHasSingleValueNotCancelled() {
		AtomicBoolean triggerCancelled = new AtomicBoolean();
		StepVerifier.create(new MonoUntilOther<>(false,
				Mono.just("foo"),
				Mono.delay(Duration.ofMillis(500))
				    .doOnCancel(() -> triggerCancelled.set(true)),
				s -> s))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(450))
		            .expectNext("foo")
		            .verifyComplete();
		assertThat(triggerCancelled.get()).isFalse();
	}

	@Test
	public void triggerSequenceDoneFirst() {
		StepVerifier.withVirtualTime(() -> new MonoUntilOther<>(false,
				Mono.delay(Duration.ofSeconds(2)),
				Mono.just("foo"),
				s -> s))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext(0L)
		            .verifyComplete();
	}

	@Test
	public void sourceHasError() {
		StepVerifier.create(new MonoUntilOther<>(false,
				Mono.<String>error(new IllegalStateException("boom")),
				Mono.just("foo"),
				s -> s))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void triggerHasError() {
		StepVerifier.create(new MonoUntilOther<>(false,
				Mono.just("foo"),
				Mono.<String>error(new IllegalStateException("boom")),
				s -> s))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void sourceAndTriggerHaveErrorsNotDelayed() {
		StepVerifier.create(new MonoUntilOther<>(false,
				Mono.<String>error(new IllegalStateException("boom1")),
				Mono.<Integer>error(new IllegalStateException("boom2")),
				s -> s))
		            .verifyErrorMessage("boom1");
	}

	@Test
	public void sourceAndTriggerHaveErrorsDelayed() {
		IllegalStateException boom1 = new IllegalStateException("boom1");
		IllegalStateException boom2 = new IllegalStateException("boom2");
		StepVerifier.create(new MonoUntilOther<>(true,
				Mono.<String>error(boom1),
				Mono.<Integer>error(boom2),
				s -> s))
		            .verifyErrorMatches(e -> e.getMessage().equals("Multiple errors") &&
				            e.getSuppressed()[0] == boom1 &&
				            e.getSuppressed()[1] == boom2);
	}

	@Test
	public void testAPIUntilOther() {
		StepVerifier.withVirtualTime(() -> Mono.just("foo")
		                                       .untilOther(Mono.delay(Duration.ofSeconds(2))))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void testAPIUntilOtherMapper() {
		StepVerifier.withVirtualTime(() -> Mono.just("foo")
		                                       .untilOther(Mono.delay(Duration.ofSeconds(2)),
				                                       s -> s + s.length()))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(2))
		            .expectNext("foo3")
		            .verifyComplete();
	}
}