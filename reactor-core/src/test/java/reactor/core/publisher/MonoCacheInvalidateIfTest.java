/*
 * Copyright (c) 2021-2024 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.test.MemoryUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.*;

class MonoCacheInvalidateIfTest {

	@Test
	void nullPredicate() {
		assertThatNullPointerException()
				.isThrownBy(() -> Mono.empty().cacheInvalidateIf(null))
				.withMessage("invalidationPredicate");
	}

	@Test
	void emptySourcePropagatesErrorAndTriggersResubscription() {
		AtomicInteger counter = new AtomicInteger();
		Mono<Integer> source = Mono.defer(() -> {
			int v = counter.incrementAndGet();
			if (v == 1) {
				return Mono.empty();
			}
			return Mono.just(v);
		});

		Mono<Integer> cached = source.cacheInvalidateIf(it -> false);

		assertThatExceptionOfType(NoSuchElementException.class)
				.as("first subscribe")
				.isThrownBy(cached::block)
				.withMessage("cacheInvalidateWhen expects a value, source completed empty");

		assertThat(cached.block())
				.as("second subscribe")
				.isEqualTo(2);
	}

	@Test
	void predicateThrowsTriggersErrorDiscardAndInvalidate() throws InterruptedException {
		AtomicInteger counter = new AtomicInteger();
		Mono<Integer> source = Mono.fromCallable(counter::incrementAndGet);

		Mono<Integer> cached = source.cacheInvalidateIf(it -> {
			if (it < 3) throw new IllegalStateException("boom");
			return false;
		});

		//first subscriber doesn't trigger the predicate
		assertThat(cached.block()).as("first subscription")
				.isEqualTo(1);

		//second subscription triggers the predicate, which throws (resulting in onError)
		StepVerifier.create(cached)
				.expectErrorSatisfies(e -> assertThat(e)
						.isInstanceOf(IllegalStateException.class)
						.hasMessage("boom"))
				.verifyThenAssertThat()
				.hasDiscarded(1);

		//third subscriber doesn't trigger the predicate
		assertThat(cached.block()).as("third subscription")
				.isEqualTo(2);

		//fourth subscription triggers the predicate again, which throws
		StepVerifier.create(cached)
				.expectErrorSatisfies(e -> assertThat(e)
						.isInstanceOf(IllegalStateException.class)
						.hasMessage("boom"))
				.verifyThenAssertThat()
				.hasDiscarded(2);

		//fifth and sixth subscriptions don't trigger the predicate anymore
		assertThat(cached.block())
				.as("5th subscribe")
				.isEqualTo(3);

		assertThat(cached.block())
				.as("6th subscribe")
				.isEqualTo(3);
	}

	@Test
	void predicatePassingLeadsToInvalidation() {
		AtomicInteger counter = new AtomicInteger();
		Mono<MemoryUtils.Tracked> source = Mono.fromCallable(() -> {
			String id = "tracked#" + counter.incrementAndGet();
			return new MemoryUtils.Tracked(id);
		});

		Mono<MemoryUtils.Tracked> cached = source.cacheInvalidateIf(MemoryUtils.Tracked::isReleased);

		MemoryUtils.Tracked first = cached.block();
		assertThat(first.identifier).as("sub1").isEqualTo("tracked#1");
		assertThat(cached.block()).as("sub2").isSameAs(first);
		assertThat(cached.block()).as("sub3").isSameAs(first);

		//trigger
		first.release();

		MemoryUtils.Tracked second = cached.block();
		assertThat(second.identifier).as("sub4")
				.isEqualTo("tracked#2")
				.isNotSameAs(first);
		assertThat(cached.block()).as("sub5").isSameAs(second);
	}

	@Test
	void cancellingAllSubscribersBeforeOnNextInvalidates() {
		TestPublisher<Integer> source = TestPublisher.create();

		Mono<Integer> cached = source
				.mono()
				.cacheInvalidateIf(i -> false);

		Disposable sub1 = cached.subscribe();
		Disposable sub2 = cached.subscribe();
		Disposable sub3 = cached.subscribe();

		source.assertWasSubscribed();
		source.assertNotCancelled();

		sub1.dispose();
		source.assertNotCancelled();

		sub2.dispose();
		source.assertNotCancelled();

		sub3.dispose();
		source.assertCancelled(1);
	}

	// See https://github.com/reactor/reactor-core/issues/3907
	@Test
	public void cancelBeforeUpstreamSubscribeDoesntFail() throws Exception {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicInteger upstreamCalled = new AtomicInteger();

		CountDownLatch cancelled = new CountDownLatch(1);
		CountDownLatch cacheInvalidateInnerSubscribed = new CountDownLatch(1);
		CountDownLatch mainChainDone = new CountDownLatch(1);

		Mono<String> cachedMono = Mono.fromSupplier(() -> {
			                              upstreamCalled.incrementAndGet();
			                              return "foobar";
		                              })
		                              .cacheInvalidateIf(c -> true);

		Mono<String> mono = Mono.defer(() -> Mono.just("foo"))
		                        .flatMap(x -> {
			                        return cachedMono
			                                          .doOnSubscribe(s -> {
				                                          cacheInvalidateInnerSubscribed.countDown();
				                                          try { cancelled.await(1, TimeUnit.SECONDS); } catch (Exception e) {}
			                                          })
			                                          .subscribeOn(Schedulers.boundedElastic());
		                        })
		                        .doOnError(error::set)
		                        .doFinally(s -> mainChainDone.countDown());

		Disposable d = mono.subscribe();

		assertThat(cacheInvalidateInnerSubscribed.await(1, TimeUnit.SECONDS))
				.as("Should assemble the chain")
				.isTrue();

		d.dispose();
		cancelled.countDown();


		assertThat(mainChainDone.await(1, TimeUnit.SECONDS))
				.as("Should finish in time")
				.isTrue();

		// Force an actual upstream subscription to validate the count
		cachedMono.block();

		assertThat(upstreamCalled.get())
				.as("No upstream subscription expected")
				.isEqualTo(1);
		assertThat(error.get())
				.as("No errors expected")
				.isNull();
	}
}
