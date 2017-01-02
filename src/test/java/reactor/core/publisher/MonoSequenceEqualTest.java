/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.concurrent.QueueSupplier;

public class MonoSequenceEqualTest {

	@Test
	public void sequenceEquals() {
		StepVerifier.create(Mono.sequenceEqual(
						Flux.just("one", "two", "three"),
						Flux.just("one", "two", "three")))
		            .expectNext(Boolean.TRUE)
		            .verifyComplete();
	}

	@Test
	public void sequenceLongerLeft() {
		StepVerifier.create(Mono.sequenceEqual(
						Flux.just("one", "two", "three", "four"),
						Flux.just("one", "two", "three")))
		            .expectNext(Boolean.FALSE)
		            .verifyComplete();
	}

	@Test
	public void sequenceLongerRight() {
		StepVerifier.create(Mono.sequenceEqual(
						Flux.just("one", "two", "three"),
						Flux.just("one", "two", "three", "four")))
		            .expectNext(Boolean.FALSE)
		            .verifyComplete();
	}

	@Test
	public void sequenceErrorsLeft() {
		StepVerifier.create(Mono.sequenceEqual(
						Flux.just("one", "two").concatWith(Mono.error(new IllegalStateException())),
						Flux.just("one", "two", "three")))
		            .verifyError(IllegalStateException.class);
	}

	@Test
	public void sequenceErrorsRight() {
		StepVerifier.create(Mono.sequenceEqual(
						Flux.just("one", "two", "three"),
						Flux.just("one", "two").concatWith(Mono.error(new IllegalStateException()))))
		            .verifyError(IllegalStateException.class);
	}

	@Test
	public void sequenceErrorsBothPropagatesLeftError() {
		StepVerifier.create(Mono.sequenceEqual(
						Flux.just("one", "two", "three").concatWith(Mono.error(new IllegalArgumentException("left"))).hide(),
						Flux.just("one", "two").concatWith(Mono.error(new IllegalArgumentException("right"))).hide()))
		            .verifyErrorMessage("left");
	}

	@Test
	public void sequenceEmptyLeft() {
		StepVerifier.create(Mono.sequenceEqual(
				Flux.empty(),
				Flux.just("one", "two", "three")))
		            .expectNext(Boolean.FALSE)
		            .verifyComplete();
	}

	@Test
	public void sequenceEmptyRight() {
		StepVerifier.create(Mono.sequenceEqual(
				Flux.just("one", "two", "three"),
				Flux.empty()))
		            .expectNext(Boolean.FALSE)
		            .verifyComplete();
	}

	@Test
	public void sequenceEmptyBoth() {
		StepVerifier.create(Mono.sequenceEqual(
				Flux.empty(),
				Flux.empty()))
		            .expectNext(Boolean.TRUE)
		            .verifyComplete();
	}

	@Test
	public void equalPredicateFailure() {
		StepVerifier.create(Mono.sequenceEqual(Mono.just("one"), Mono.just("one"),
						(s1, s2) -> { throw new IllegalStateException("boom"); }))
		            .verifyErrorMessage("boom");
	}

	@Test
	public void largeSequence() {
		Flux<Integer> source = Flux.range(1, QueueSupplier.SMALL_BUFFER_SIZE * 4).subscribeOn(Schedulers.elastic());

		StepVerifier.create(Mono.sequenceEqual(source, source))
		            .expectNext(Boolean.TRUE)
		            .expectComplete()
		            .verify(Duration.ofSeconds(5));
	}

		@Test
	public void syncFusedCrash() {
		Flux<Integer> source = Flux.range(1, 10).map(i -> { throw new IllegalArgumentException("boom"); });

		StepVerifier.create(Mono.sequenceEqual(source, Flux.range(1, 10).hide()))
		            .verifyErrorMessage("boom");

		StepVerifier.create(Mono.sequenceEqual(Flux.range(1, 10).hide(), source))
		            .verifyErrorMessage("boom");
	}

	//TODO multithreaded race between cancel and onNext, between cancel and drain, source overflow, error dropping to hook
}