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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MonoPeekTest {

	@Test
	public void normal() {
		//FIXME
	}

	@Test
	public void monoAfterTerminateValue() {
		AtomicBoolean success = new AtomicBoolean(false);

		StepVerifier.create(Mono.just("foo").hide()
		                        .doAfterTerminate((v, t) -> success.set("foo".equals(v) && t == null)))
		            .expectNoFusionSupport()
		            .expectNext("foo")
		            .expectComplete()
		            .verify();

		assertTrue("expected doAfterTerminate to receive value \"foo\" and null throwable",
				success.get());
	}

	@Test
	public void monoAfterTerminateEmpty() {
		AtomicBoolean success = new AtomicBoolean(false);

		StepVerifier.create(Mono.empty().hide()
		                        .doAfterTerminate((v, t) -> success.set(v == null && t == null)))
		            .expectNoFusionSupport()
		            .expectComplete()
		            .verify();

		assertTrue("expected doAfterTerminate to receive null value and null throwable",
				success.get());
	}

	@Test
	public void monoAfterTerminateError() {
		IllegalArgumentException err = new IllegalArgumentException("foo");
		AtomicBoolean success = new AtomicBoolean(false);

		StepVerifier.create(Mono.error(err).hide()
		                        .doAfterTerminate((v, t) -> success.set(v == null && t == err)))
		            .expectNoFusionSupport()
		            .expectError(IllegalArgumentException.class)
		            .verify();

		assertTrue("expected doAfterTerminate to receive null value and IllegalArgumentException throwable",
				success.get());
	}

	@Test
	public void monoAfterTerminateCancel() {
		AtomicBoolean success = new AtomicBoolean(false);
		AtomicBoolean cancelCheck = new AtomicBoolean(false);
		LongAdder afterTerminateCount = new LongAdder();

		StepVerifier.create(Mono.just(1).hide()
		                        .doOnCancel(() -> cancelCheck.set(true))
		                        .doAfterTerminate((v, t) -> {
			                        afterTerminateCount.increment();
			                        success.set(v == 1 && t == null);
		                        }))
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .thenCancel()
		            .verify();

		assertEquals("expected doAfterTerminate to be invoked exactly once",
				1, afterTerminateCount.longValue());
		assertTrue("expected doAfterTerminate to receive value 1 and null throwable",
				success.get());
		assertTrue("expected tested mono to be cancelled", cancelCheck.get());
	}

	@Test
	public void fuseableMonoAfterTerminateValue() {
		AtomicBoolean success = new AtomicBoolean(false);

		StepVerifier.create(Mono.just("foo")
		                        .doAfterTerminate((v, t) -> success.set("foo".equals(v) && t == null)))
		            .expectFusion()
		            .expectNext("foo")
		            .expectComplete()
		            .verify();

		assertTrue("expected doAfterTerminate to receive value \"foo\" and null throwable",
				success.get());
	}

	//TODO if fuseable Mono with empty value or error surface, test them here

	@Test
	public void fuseableMonoAfterTerminateCancel() {
		AtomicBoolean afterTerminateCheck = new AtomicBoolean(false);
		AtomicBoolean completeCheck = new AtomicBoolean(false);
		LongAdder cancelCheck = new LongAdder();
		AtomicReference<Subscription> subscription = new AtomicReference<>(null);
		LongAdder afterTerminateCount = new LongAdder();

		Mono<Integer> mono = Mono.just(1)
		                         .doOnSubscribe(subscription::set)
		                         //doing this here will help triggering cancel() and not onComplete()...
		                         .doOnNext(l -> subscription.get().cancel())
		                         .doOnCancel(cancelCheck::increment)
		                         .doAfterTerminate((v, t) -> {
			                         afterTerminateCount.increment();
			                         afterTerminateCheck.set(v == 1 && t == null);
		                         });

		StepVerifier.create(mono.log(), 0)
		            .expectFusion(Fuseable.NONE, Fuseable.ANY) //assert fusion but don't trigger it so we may cancel
		            .thenRequest(1)
		            .expectNext(1)
		            .thenCancel() //... but we still need a terminal step
		            .verify();

		assertEquals("expected tested Mono to be cancelled exactly once",
				1, cancelCheck.longValue());
		assertFalse("expected tested Mono to not complete", completeCheck.get());
		assertEquals("expected doAfterTerminate to be invoked exactly once",
				1, afterTerminateCount.longValue());
		assertTrue("expected doAfterTerminate to receive value 1 and null throwable",
				afterTerminateCheck.get());
	}

	//see https://github.com/reactor/reactor-core/issues/251
	@Test
	public void testElasticDoAfterTerminate() throws InterruptedException {
		AtomicBoolean afterTerminateCheck = new AtomicBoolean(false);
		CountDownLatch latch = new CountDownLatch(1);
		Mono<String> mono  = Mono.delay(Duration.ofMillis(300))
		    .then((x) -> Mono.just("Hello World"))
		    .doAfterTerminate((result, error) -> {
			    afterTerminateCheck.set(true);
			    System.out.println("Validation terminated: "+result+", "+error);
			    latch.countDown();
		    });

		StepVerifier.create(mono.subscribeOn(Schedulers.elastic()))
		            .expectNoFusionSupport()
		            .expectNext("Hello World")
		            .expectComplete()
		            .verify();

		//since we're on a separate thread, it could be that this assertion is run just
		//between verify's termination and the invocation of doAfterTerminate, so we use a latch
		latch.await(300, TimeUnit.MILLISECONDS);
		assertTrue("Expected afterTerminate to have been invoked", afterTerminateCheck.get());
	}

}