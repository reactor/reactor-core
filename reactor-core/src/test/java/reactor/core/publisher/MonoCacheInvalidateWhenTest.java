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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.*;

class MonoCacheInvalidateWhenTest {

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
				.withMessage("foo");

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
				.hasMessage(""))
				.verifyThenAssertThat()
				.hasDiscarded(1);

		assertThatNullPointerException()
				.as("first subscribe")
				.isThrownBy(cached::block)
				.withMessage("foo");

		assertThat(cached.block())
				.as("second subscribe")
				.isEqualTo(2);
	}

	@Test
	void generatorThrowsTriggersErrorDiscardAndResubscribe() {
		AtomicInteger counter = new AtomicInteger();
		Mono<Integer> source = Mono.fromCallable(counter::incrementAndGet);

		Mono<Integer> cached = source.cacheInvalidateWhen(it -> {
			if (it == 1) throw new IllegalStateException("boom");
			return Mono.never();
		});

		StepVerifier.create(cached)
				.expectErrorSatisfies(e -> assertThat(e)
						.isInstanceOf(IllegalStateException.class)
						.hasMessage("boom"))
				.verifyThenAssertThat()
				.hasDiscarded(1);

		assertThatNullPointerException()
				.as("first subscribe")
				.isThrownBy(cached::block)
				.withMessage("foo");

		assertThat(cached.block())
				.as("second subscribe")
				.isEqualTo(2);
	}

	@Test
	void triggerCompletingLeadsToInvalidation() {
		AtomicInteger counter = new AtomicInteger();
		Mono<Integer> source = Mono.fromCallable(counter::incrementAndGet);

		final Sinks.Empty<Void> trigger = Sinks.empty();
		Mono<Integer> cached = source.cacheInvalidateWhen(it -> trigger.asMono());

		assertThat(cached.block()).as("sub1").isEqualTo(1);
		assertThat(cached.block()).as("sub2").isEqualTo(1);
		assertThat(cached.block()).as("sub3").isEqualTo(1);

		//trigger (let's still assert the result)
		Sinks.EmitResult triggerResult = trigger.tryEmitEmpty();
		assertThat(triggerResult).isEqualTo(Sinks.EmitResult.OK);

		assertThat(cached.block()).as("sub4").isEqualTo(2);
		assertThat(cached.block()).as("sub5").isEqualTo(2);
	}

	@Test
	void triggerFailingLeadsToInvalidation() {
		AtomicInteger counter = new AtomicInteger();
		Mono<Integer> source = Mono.fromCallable(counter::incrementAndGet);

		final Sinks.Empty<Void> trigger = Sinks.empty();
		Mono<Integer> cached = source.cacheInvalidateWhen(it -> trigger.asMono());

		assertThat(cached.block()).as("sub1").isEqualTo(1);
		assertThat(cached.block()).as("sub2").isEqualTo(1);
		assertThat(cached.block()).as("sub3").isEqualTo(1);

		List<Throwable> errorsDropped = new ArrayList<>();
		Hooks.onErrorDropped(errorsDropped::add); //will be reset by ReactorTestExecutionListener

		//trigger (let's still assert the result)
		Sinks.EmitResult triggerResult = trigger.tryEmitError(new IllegalStateException("expected dropped"));
		assertThat(triggerResult).isEqualTo(Sinks.EmitResult.OK);

		assertThat(cached.block()).as("sub4").isEqualTo(2);
		assertThat(cached.block()).as("sub5").isEqualTo(2);
	}

	@Test
	void concurrentTriggerAndDownstreamSubscriber() {
		//FIXME
		fail("TO BE IMPLEMENTED");
	}
}