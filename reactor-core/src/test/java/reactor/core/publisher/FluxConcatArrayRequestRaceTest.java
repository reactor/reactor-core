/*
 * Copyright (c) 2026 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;

import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;

class FluxConcatArrayRequestRaceTest {

	@ParameterizedTest(name = "{displayName} [{index}] {arguments}")
	@ValueSource(longs = {1, Long.MAX_VALUE})
	void requestIsNotLostWhenEmptySourceCompletes(long requested) {
		for (int i = 0; i < 10_000; i++) {
			TestPublisher<Integer> first = TestPublisher.create();
			AssertSubscriber<Integer> subscriber = AssertSubscriber.create(0);

			Flux.concat(first, Flux.just(1).hide()).subscribe(subscriber);

			RaceTestUtils.race(first::complete, () -> subscriber.request(requested));

			subscriber.assertValues(1).assertComplete().assertNoError();
		}
	}

	@ParameterizedTest(name = "{displayName} [{index}] {arguments}")
	@ValueSource(longs = {1, Long.MAX_VALUE})
	void delayErrorRequestIsNotLostWhenEmptySourceCompletes(long requested) {
		for (int i = 0; i < 10_000; i++) {
			TestPublisher<Integer> first = TestPublisher.create();
			AssertSubscriber<Integer> subscriber = AssertSubscriber.create(0);

			Flux.concatDelayError(first, Flux.just(1).hide()).subscribe(subscriber);

			RaceTestUtils.race(first::complete, () -> subscriber.request(requested));

			subscriber.assertValues(1).assertComplete().assertNoError();
		}
	}

	@ParameterizedTest(name = "{displayName} [{index}] {arguments}")
	@ValueSource(booleans = {false, true})
	void requestRemainsBoundedAcrossConsecutiveEmptySources(boolean delayError) {
		for (int i = 0; i < 10_000; i++) {
			TestPublisher<Integer> first = TestPublisher.create();
			AssertSubscriber<Integer> subscriber = AssertSubscriber.create(0);
			Flux<Integer> concat = delayError
					? Flux.concatDelayError(first, Flux.empty(), Flux.empty(), Flux.range(1, 2).hide())
					: Flux.concat(first, Flux.empty(), Flux.empty(), Flux.range(1, 2).hide());

			concat.subscribe(subscriber);

			RaceTestUtils.race(first::complete, () -> subscriber.request(1));

			subscriber.assertValues(1).assertNotComplete().assertNoError();

			subscriber.request(1);

			subscriber.assertValues(1, 2).assertComplete().assertNoError();
		}
	}

	@ParameterizedTest(name = "{displayName} [{index}] {arguments}")
	@ValueSource(booleans = {false, true})
	void cancellationDoesNotLeaveNextSourceSubscribed(boolean delayError) {
		for (int i = 0; i < 1000; i++) {
			TestPublisher<Integer> first = TestPublisher.create();
			TestPublisher<Integer> second = TestPublisher.create();
			AssertSubscriber<Integer> subscriber = AssertSubscriber.create(0);
			Flux<Integer> concat = delayError
					? Flux.concatDelayError(first, second)
					: Flux.concat(first, second);

			concat.subscribe(subscriber);

			RaceTestUtils.race(first::complete, subscriber::cancel);

			first.assertNoSubscribers();
			second.assertNoSubscribers();
			subscriber.assertNoValues().assertNotComplete().assertNoError();
		}
	}

	@ParameterizedTest(name = "{displayName} [{index}] {arguments}")
	@ValueSource(booleans = {false, true})
	void requestIsPreservedWhenNonEmptySourceCompletes(boolean delayError) {
		for (int i = 0; i < 1000; i++) {
			TestPublisher<Integer> first = TestPublisher.create();
			AssertSubscriber<Integer> subscriber = AssertSubscriber.create(1);
			Flux<Integer> concat = delayError
					? Flux.concatDelayError(first, Flux.range(2, 2).hide())
					: Flux.concat(first, Flux.range(2, 2).hide());

			concat.subscribe(subscriber);
			first.next(1);

			RaceTestUtils.race(first::complete, () -> subscriber.request(1));

			subscriber.assertValues(1, 2).assertNotComplete().assertNoError();

			subscriber.request(1);

			subscriber.assertValues(1, 2, 3).assertComplete().assertNoError();
		}
	}

	@ParameterizedTest(name = "{displayName} [{index}] {arguments}")
	@ValueSource(booleans = {false, true})
	void reentrantRequestIsPreservedAcrossSources(boolean delayError) {
		AssertSubscriber<Integer> subscriber = new AssertSubscriber<Integer>(1) {
			@Override
			public void onNext(Integer value) {
				super.onNext(value);
				request(1);
			}
		};
		Flux<Integer> concat = delayError
				? Flux.concatDelayError(Flux.range(1, 2).hide(), Flux.empty(),
						Flux.range(3, 2).hide(), Flux.empty(), Flux.just(5).hide())
				: Flux.concat(Flux.range(1, 2).hide(), Flux.empty(),
						Flux.range(3, 2).hide(), Flux.empty(), Flux.just(5).hide());

		concat.subscribe(subscriber);

		subscriber.assertValues(1, 2, 3, 4, 5).assertComplete().assertNoError();
	}

	@ParameterizedTest(name = "{displayName} [{index}] {arguments}")
	@ValueSource(booleans = {false, true})
	void manySynchronousEmptySourcesCompleteWithoutStackOverflow(boolean delayError) {
		@SuppressWarnings("unchecked")
		Publisher<Integer>[] sources = (Publisher<Integer>[]) new Publisher<?>[10_001];
		Arrays.fill(sources, Flux.empty());
		sources[sources.length - 1] = Flux.just(1).hide();
		AssertSubscriber<Integer> subscriber = AssertSubscriber.create(1);

		Flux<Integer> concat = delayError ? Flux.concatDelayError(sources) : Flux.concat(sources);
		concat.subscribe(subscriber);

		subscriber.assertSubscribed().assertValues(1).assertComplete().assertNoError();
	}

	@Test
	void delayedErrorDoesNotLoseRacingRequestForNextSource() {
		IllegalStateException failure = new IllegalStateException("boom");
		for (int i = 0; i < 1000; i++) {
			TestPublisher<Integer> first = TestPublisher.create();
			AssertSubscriber<Integer> subscriber = AssertSubscriber.create(0);

			Flux.concatDelayError(first, Flux.just(1).hide()).subscribe(subscriber);

			RaceTestUtils.race(() -> first.error(failure), () -> subscriber.request(1));

			subscriber.assertValues(1)
					.assertError(IllegalStateException.class)
					.assertErrorMessage("boom")
					.assertNotComplete();
		}
	}
}
