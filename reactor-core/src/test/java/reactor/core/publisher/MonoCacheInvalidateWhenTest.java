/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import reactor.core.Disposable;
import reactor.test.AutoDisposingExtension;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assumptions.assumeThat;

class MonoCacheInvalidateWhenTest {

	@RegisterExtension
	AutoDisposingExtension afterTest = new AutoDisposingExtension();

	@Test
	void nullGenerator() {
		assertThatNullPointerException()
				.isThrownBy(() -> Mono.empty().cacheInvalidateWhen(null))
				.withMessage("invalidationTriggerGenerator");
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

		Mono<Integer> cached = source.cacheInvalidateWhen(it -> Mono.never());

		assertThatExceptionOfType(NoSuchElementException.class)
				.as("first subscribe")
				.isThrownBy(cached::block)
				.withMessage("cacheInvalidateWhen expects a value, source completed empty");

		assertThat(cached.block())
				.as("second subscribe")
				.isEqualTo(2);
	}

	@Test
	void generatorReturnsNullTriggersErrorAndResubscribeAndInvalidateHandler() {
		AtomicInteger counter = new AtomicInteger();
		Mono<Integer> source = Mono.fromCallable(counter::incrementAndGet);
		CopyOnWriteArrayList<Integer> invalidateHandler = new CopyOnWriteArrayList<>();

		Mono<Integer> cached = source.cacheInvalidateWhen(it -> {
			if (it == 1) return null;
			return Mono.never();
		}, invalidateHandler::add);

		StepVerifier.create(cached)
				.expectErrorSatisfies(e -> assertThat(e)
				.isInstanceOf(NullPointerException.class)
				.hasMessage("invalidationTriggerGenerator produced a null trigger"))
				.verifyThenAssertThat()
				.hasNotDiscardedElements();

		assertThat(invalidateHandler).as("invalidateHandler").containsExactly(1);

		assertThat(cached.block())
				.as("second subscribe")
				.isEqualTo(2);

		assertThat(cached.block())
				.as("third subscribe")
				.isEqualTo(2);
	}

	@Test
	void generatorThrowsTriggersErrorAndInvalidateAndInvalidateHandler() {
		AtomicInteger counter = new AtomicInteger();
		Mono<Integer> source = Mono.fromCallable(counter::incrementAndGet);
		CopyOnWriteArrayList<Integer> invalidateHandler = new CopyOnWriteArrayList<>();

		Mono<Integer> cached = source.cacheInvalidateWhen(it -> {
			if (it < 3) throw new IllegalStateException("boom");
			return Mono.never();
		}, invalidateHandler::add);

		StepVerifier.create(cached)
				.expectErrorSatisfies(e -> assertThat(e)
						.isInstanceOf(IllegalStateException.class)
						.hasMessage("boom"))
				.verifyThenAssertThat()
				.hasNotDiscardedElements();

		assertThat(invalidateHandler).as("after first subscription").containsExactly(1);

		StepVerifier.create(cached)
				.expectErrorSatisfies(e -> assertThat(e)
						.isInstanceOf(IllegalStateException.class)
						.hasMessage("boom"))
				.verifyThenAssertThat()
				.hasNotDiscardedElements();

		assertThat(invalidateHandler).as("after second subscription").containsExactly(1, 2);

		assertThat(cached.block())
				.as("third subscribe")
				.isEqualTo(3);
	}

	@Test
	void triggerCompletingLeadsToInvalidation() {
		AtomicInteger counter = new AtomicInteger();
		Mono<Integer> source = Mono.fromCallable(counter::incrementAndGet);

		//we want to be able to reset it
		final TestPublisher<Void> trigger = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		Mono<Integer> cached = source.cacheInvalidateWhen(it -> trigger.mono());

		assertThat(cached.block()).as("sub1").isEqualTo(1);
		assertThat(cached.block()).as("sub2").isEqualTo(1);
		assertThat(cached.block()).as("sub3").isEqualTo(1);

		//trigger
		trigger.complete();

		assertThat(cached.block()).as("sub4").isEqualTo(2);
		assertThat(cached.block()).as("sub5").isEqualTo(2);
	}

	@Test
	void triggerFailingLeadsToInvalidation() {
		AtomicInteger counter = new AtomicInteger();
		Mono<Integer> source = Mono.fromCallable(counter::incrementAndGet);

		//we want to be able to reset it
		final TestPublisher<Void> trigger = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		Mono<Integer> cached = source.cacheInvalidateWhen(it -> trigger.mono());

		assertThat(cached.block()).as("sub1").isEqualTo(1);
		assertThat(cached.block()).as("sub2").isEqualTo(1);
		assertThat(cached.block()).as("sub3").isEqualTo(1);

		List<Throwable> errorsDropped = new ArrayList<>();
		Hooks.onErrorDropped(errorsDropped::add); //will be reset by ReactorTestExecutionListener
		trigger.error(new IllegalStateException("expected logged"));

		assertThat(cached.block()).as("sub4").isEqualTo(2);
		assertThat(cached.block()).as("sub5").isEqualTo(2);

		assertThat(errorsDropped).as("errorsDropped").isEmpty();
	}

	@Test
	void triggerInvalidationAppliesInvalidationHandler() {
		AtomicInteger counter = new AtomicInteger();
		Mono<Integer> source = Mono.fromCallable(counter::incrementAndGet);
		AtomicReference<Integer> invalidationHandler = new AtomicReference<>();

		final TestPublisher<Void> trigger = TestPublisher.create();
		Mono<Integer> cached = source.cacheInvalidateWhen(it -> trigger.mono(), invalidationHandler::set);

		assertThat(cached.block()).as("sub1").isEqualTo(1);

		//trigger
		trigger.complete();

		assertThat(invalidationHandler).as("invalidated").hasValue(1);
	}

	@Test
	void invalidationHandlerProtectedAgainstException() {
		AtomicInteger counter = new AtomicInteger();
		Mono<Integer> source = Mono.fromCallable(counter::incrementAndGet);

		final TestPublisher<Void> trigger = TestPublisher.create();
		Mono<Integer> cached = source.cacheInvalidateWhen(it -> trigger.mono(),
				discarded -> {
					throw new IllegalStateException("boom invalidation handler");
				});

		assertThat(cached.block()).as("sub1").isEqualTo(1);

		//trigger
		assertThatCode(trigger::complete).doesNotThrowAnyException();
	}

	@Test
	void cancellingAllSubscribersBeforeOnNextInvalidates() {
		TestPublisher<Integer> source = TestPublisher.create();

		Mono<Integer> cached = source
				.mono()
				.cacheInvalidateWhen(i -> Mono.never());

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

	@Test
	void concurrentTriggerAndDownstreamSubscriber() {
		int lessInteresting = 0;
		final int loops = 1000;
		for (int i = 0; i < loops; i++) {
			final TestPublisher<Void> trigger = TestPublisher.create();

			AtomicInteger counter = new AtomicInteger();
			Mono<Integer> source = Mono.fromCallable(counter::incrementAndGet);

			Mono<Integer> cached = source.cacheInvalidateWhen(it -> trigger.mono());

			assertThat(cached.block()).as("sub1").isEqualTo(1);

			AtomicReference<Integer> sub2Ref = new AtomicReference<>();

			RaceTestUtils.race(
					() -> cached.subscribe(sub2Ref::set),
					trigger::complete
			);

			Integer sub2Seen = sub2Ref.get();
			assertThat(sub2Seen).as("sub2 seen in round " + i)
					.isNotNull()
					.isBetween(1, 2);
			if (sub2Seen == 1) lessInteresting++;
		}
		//smoke assertion: we caught more than 50% of interesting cases
		//however there is no guarantee we can't see more
		assumeThat(lessInteresting)
				.as("less interesting cases (got 1)")
				.isBetween(0, loops * 50 / 100);
	}
}
