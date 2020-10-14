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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThat;

public class MonoRepeatTest {

	@Test
	public void timesInvalid() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			Mono.never()
					.repeat(-1);
		});
	}

	@Test
	public void zeroRepeat() {
		AtomicInteger i = new AtomicInteger();

		StepVerifier.create(Mono.fromCallable(i::incrementAndGet)
		                        .repeat(0))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void oneRepeat() {
		AtomicInteger i = new AtomicInteger();

		StepVerifier.create(Mono.fromCallable(i::incrementAndGet)
		                        .repeat(1))
		            .expectNext(1, 2)
		            .verifyComplete();
	}

	@Test
	public void oneRepeatBackpressured() {
		AtomicInteger i = new AtomicInteger();

		StepVerifier.create(Mono.fromCallable(i::incrementAndGet)
		                        .repeat(1), 0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenRequest(3)
		            .expectNext(1, 2)
		            .verifyComplete();
	}

	@Test
	public void twoRepeat() {
		AtomicInteger i = new AtomicInteger();

		StepVerifier.create(Mono.fromCallable(i::incrementAndGet)
		                        .repeat(2))
		            .expectNext(1, 2, 3)
		            .verifyComplete();
	}

	@Test
	public void twoRepeatNormal() {
		AtomicInteger i = new AtomicInteger();

		StepVerifier.create(Mono.fromCallable(i::incrementAndGet)
		                        .repeat(2)
		                        .count())
		            .expectNext(3L)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void twoRepeatNormalSupplier() {
		AtomicInteger i = new AtomicInteger();
		AtomicBoolean bool = new AtomicBoolean(true);

		StepVerifier.create(Mono.fromCallable(i::incrementAndGet)
		                        .repeat(2, bool::get))
		            .expectNext(1, 2)
		            .expectNext(3)
		            .then(() -> bool.set(false))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void twoRepeatBackpressured() {
		AtomicInteger i = new AtomicInteger();

		StepVerifier.create(Mono.fromCallable(i::incrementAndGet)
		                        .repeat(2), 0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenRequest(2)
		            .expectNext(1, 2)
		            .thenRequest(3)
		            .expectNext(3)
		            .verifyComplete();
	}

	@Test
	public void repeatInfinite() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger i = new AtomicInteger();
		Mono.fromCallable(i::incrementAndGet)
		    .repeat()
		    .take(9)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void scanOperator(){
	    MonoRepeat<Integer> test = new MonoRepeat<>(Mono.just(1), 5L);

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
