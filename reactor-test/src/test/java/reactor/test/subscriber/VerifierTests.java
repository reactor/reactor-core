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
package reactor.test.subscriber;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.scheduler.VirtualTimeScheduler;

/**
 * @author Stephane Maldini
 */
public class VerifierTests {


	@Test
	public void verifyVirtualTimeOnSubscribe() {
		Verifier.with(() -> Mono.delay(Duration.ofDays(2))
		                        .map(l -> "foo"))
		        .thenAwait(Duration.ofDays(3))
		        .expectNext("foo")
		        .expectComplete()
		        .verify();
	}

	@Test
	public void verifyVirtualTimeOnError() {
		Verifier.with(() -> Mono.never()
		                        .timeout(Duration.ofDays(2))
		                        .map(l -> "foo"))
		        .thenAwait(Duration.ofDays(2))
		        .expectError(TimeoutException.class)
		        .verify();

	}

	@Test
	public void verifyVirtualTimeOnNext() {
		Verifier.with(() -> Flux.just("foo", "bar", "foobar")
		                        .delay(Duration.ofHours(1))
		                        .log())
		        .thenAwait(Duration.ofHours(1))
		        .expectNext("foo")
		        .thenAwait(Duration.ofHours(1))
		        .expectNext("bar")
		        .thenAwait(Duration.ofHours(1))
		        .expectNext("foobar")
		        .expectComplete()
		        .verify();

	}

	@Test
	public void verifyVirtualTimeOnComplete() {
		Verifier.with(() -> Flux.empty()
		                        .delaySubscription(Duration.ofHours(1))
		                        .log())
		        .thenAwait(Duration.ofHours(1))
		        .expectComplete()
		        .verify();

	}

	@Test
	public void verifyVirtualTimeOnNextInterval() {
		Duration r;

		r = Verifier.with(() -> Flux.interval(Duration.ofSeconds(3))
		                        .map(d -> "t" + d))
		        .thenAwait(Duration.ofSeconds(3))
		        .expectNext("t0")
		        .thenAwait(Duration.ofSeconds(3))
		        .expectNext("t1")
		        .thenAwait(Duration.ofSeconds(3))
		        .expectNext("t2")
		        .thenCancel()
		        .verify();

		Assert.assertTrue(r.minus(Duration.ofSeconds(9))
		                   .isNegative());
	}

	@Test
	public void verifyVirtualTimeOnNextIntervalReal() {
		Duration r;

		r = Verifier.with(3,
				() -> Flux.interval(Duration.ofSeconds(2))
				          .map(d -> "t" + d),
				null)
		            .thenAwait(Duration.ofSeconds(2))
		            .expectNext("t0")
		            .thenAwait(Duration.ofSeconds(2))
		            .expectNext("t1")
		            .thenCancel()
		            .verify();

		Assert.assertFalse(r.minus(Duration.ofSeconds(4))
		                    .isNegative());
	}

	@Test(timeout = 3000)
	public void verifyVirtualTimeOnNextIntervalManual() {
		VirtualTimeScheduler vts = VirtualTimeScheduler.create();

		Verifier.with(() -> Flux.intervalMillis(1000, vts)
		                        .map(d -> "t" + d))
		        .then(() -> vts.advanceTimeBy(Duration.ofHours(1)))
		        .expectNextCount(3600)
		        .thenCancel()
		        .verify();
	}


	@Test
	public void verifyVirtualTimeOnErrorInterval() {
		Verifier.with(0, () -> Flux.interval(Duration.ofSeconds(3))
		                        .map(d -> "t" + d))
		        .thenRequest(1)
		        .thenAwait(Duration.ofSeconds(3))
		        .expectNext("t0")
		        .thenRequest(1)
		        .thenAwait(Duration.ofSeconds(3))
		        .expectNext("t1")
		        .thenAwait(Duration.ofSeconds(3))
		        .expectError(IllegalStateException.class)
		        .verify();

	}
}
