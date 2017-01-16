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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoDelayElementTest {

	@Test
	public void normalIsDelayed() {
		Mono<String> source = Mono.just("foo").log().hide();

		StepVerifier.withVirtualTime(() -> new MonoDelayElement<>(source, 2, TimeUnit.SECONDS,
				Schedulers.timer()).log())
	                .expectSubscription()
	                .expectNoEvent(Duration.ofSeconds(2))
	                .expectNext("foo")
	                .verifyComplete();
	}

	@Test
	public void cancelDuringDelay() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();
		AtomicBoolean emitted = new AtomicBoolean();
		Mono<String> source = Mono.just("foo").log().hide();

		StepVerifier.withVirtualTime(
				() -> new MonoDelayElement<>(source, 2, TimeUnit.SECONDS, vts)
						.log()
						.doOnNext(n -> emitted.set(true)),
				() -> vts, Long.MAX_VALUE)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(1))
		            .thenCancel()
		            .verify();

		vts.advanceTimeBy(Duration.ofHours(1));
		assertThat(emitted.get()).isFalse();
	}

	@Test(timeout = 5000L)
	public void emptyIsImmediate() {
		Mono<String> source = Mono.<String>empty().log().hide();

		Duration d = StepVerifier.create(new MonoDelayElement<>(source, 10, TimeUnit.SECONDS,
				Schedulers.timer()).log())
		            .expectSubscription()
		            .verifyComplete();

		assertThat(d).isLessThan(Duration.ofSeconds(1));
	}

	@Test
	public void errorIsImmediate() {
		Mono<String> source = Mono.<String>error(new IllegalStateException("boom")).hide();

		Duration d = StepVerifier.create(new MonoDelayElement<>(source, 10, TimeUnit.SECONDS, Schedulers.timer()).log())
		                         .expectSubscription()
		                         .verifyErrorMessage("boom");

		assertThat(d).isLessThan(Duration.ofSeconds(1));
	}

	@Test
	public void errorAfterNextIsNeverTriggered() {
		TestPublisher<String> source = TestPublisher.create();
		AtomicReference<Throwable> errorDropped = new AtomicReference<>();
		Hooks.onErrorDropped(errorDropped::set);

		try {
			StepVerifier.withVirtualTime(() ->
					new MonoDelayElement<>(source, 2, TimeUnit.SECONDS, Schedulers.timer()))
			            .expectSubscription()
			            .then(() -> source.next("foo").error(new IllegalStateException("boom")))
			            .expectNoEvent(Duration.ofSeconds(2))
			            .expectNext("foo")
			            .verifyComplete();
		} finally {
			Hooks.resetOnErrorDropped();
		}

		assertThat(errorDropped.get()).isNull();
	}

}