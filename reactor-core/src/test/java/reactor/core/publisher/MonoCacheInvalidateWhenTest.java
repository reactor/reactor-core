/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import reactor.core.Disposable;
import reactor.test.AutoDisposingExtension;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.LoggerUtils;
import reactor.test.util.TestLogger;

import static org.assertj.core.api.Assertions.*;

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
	void generatorReturnsNullTriggersErrorDiscardAndResubscribe() {
		AtomicInteger counter = new AtomicInteger();
		Mono<Integer> source = Mono.fromCallable(counter::incrementAndGet);

		Mono<Integer> cached = source.cacheInvalidateWhen(it -> {
			if (it == 1) return null;
			return Mono.never();
		});

		StepVerifier.create(cached)
				.expectErrorSatisfies(e -> assertThat(e)
				.isInstanceOf(NullPointerException.class)
				.hasMessage("invalidationTriggerGenerator produced a null trigger"))
				.verifyThenAssertThat()
				.hasDiscarded(1);

		assertThat(cached.block())
				.as("second subscribe")
				.isEqualTo(2);

		assertThat(cached.block())
				.as("third subscribe")
				.isEqualTo(2);
	}

	@Test
	void generatorThrowsTriggersErrorDiscardAndInvalidate() {
		AtomicInteger counter = new AtomicInteger();
		Mono<Integer> source = Mono.fromCallable(counter::incrementAndGet);

		Mono<Integer> cached = source.cacheInvalidateWhen(it -> {
			if (it < 3) throw new IllegalStateException("boom");
			return Mono.never();
		});

		StepVerifier.create(cached)
				.expectErrorSatisfies(e -> assertThat(e)
						.isInstanceOf(IllegalStateException.class)
						.hasMessage("boom"))
				.verifyThenAssertThat()
				.hasDiscarded(1);

		StepVerifier.create(cached)
				.expectErrorSatisfies(e -> assertThat(e)
						.isInstanceOf(IllegalStateException.class)
						.hasMessage("boom"))
				.verifyThenAssertThat()
				.hasDiscarded(2);

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
		TestLogger testLogger = new TestLogger(false);
		LoggerUtils.enableCaptureWith(testLogger);
		afterTest.autoDispose(LoggerUtils::disableCapture);

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

		assertThat(Arrays.stream(testLogger.getErrContent().split("\n")))
				.as("trace log of trigger error")
				.filteredOn(s -> s.contains("expected"))
				.allMatch(s -> s.endsWith("Invalidation triggered by onError(java.lang.IllegalStateException: " +
						"expected logged)"))
				.hasSize(1);

		assertThat(cached.block()).as("sub4").isEqualTo(2);
		assertThat(cached.block()).as("sub5").isEqualTo(2);

		assertThat(errorsDropped).as("errorsDropped").isEmpty();
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
		//FIXME
		fail("TO BE IMPLEMENTED");
	}
}