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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.publisher.Sinks.Emission;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class SinkManyBestEffortTest {

	@Test
	void currentContextReflectSubscriberContext() {
		SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();

		assertThat(sink.currentContext()).isSameAs(Context.empty());

		AssertSubscriber<Integer> subscriber1 = new AssertSubscriber<>(Context.of("key", "value1"));
		AssertSubscriber<Integer> subscriber2 = new AssertSubscriber<>(Context.of("key", "value2"));
		sink.subscribe(subscriber1);
		sink.subscribe(subscriber2);

		assertThat(sink.currentContext()).isEqualTo(subscriber1.currentContext());
	}

	@Test
	void tryEmitNextNoSubscribers() {
		SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();

		assertThat(sink.tryEmitNext(1)).as("tryEmitNext").isEqualTo(Emission.FAIL_ZERO_SUBSCRIBER);
	}

	@Test
	void tryEmitNextOnTerminated() {
		SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();
		sink.tryEmitComplete().orThrow();

		assertThat(sink.tryEmitNext(1)).as("tryEmitNext").isEqualTo(Emission.FAIL_TERMINATED);
	}

	@Test
	void tryEmitCompleteNoSubscribersRetains() {
		SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();

		assertThat(sink.tryEmitComplete()).as("tryEmitComplete").isEqualTo(Emission.OK);

		StepVerifier.create(sink.asFlux())
		            .verifyComplete();
	}

	@Test
	void tryEmitCompleteOnTerminated() {
		SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();
		sink.tryEmitComplete().orThrow();

		assertThat(sink.tryEmitComplete()).as("tryEmitComplete").isEqualTo(Emission.FAIL_TERMINATED);
	}

	@Test
	void tryEmitErrorNoSubscribersRetains() {
		SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();

		assertThat(sink.tryEmitError(new IllegalStateException("boom"))).as("tryEmitError").isEqualTo(Emission.OK);

		StepVerifier.create(sink.asFlux())
		            .verifyErrorMessage("boom");
	}

	@Test
	void tryEmitErrorOnTerminated() {
		SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();
		sink.tryEmitError(new IllegalStateException("boom")).orThrow();

		assertThat(sink.tryEmitError(new IllegalStateException("boom ignored"))).as("tryEmitError").isEqualTo(Emission.FAIL_TERMINATED);

		StepVerifier.create(sink.asFlux())
		            .verifyErrorMessage("boom");
	}

	@Test
	void tryEmitErrorOnSubscribers() {
		SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();

		AssertSubscriber<Integer> sub1 = AssertSubscriber.create();
		AssertSubscriber<Integer> sub2 = AssertSubscriber.create(0);
		sink.subscribe(sub1);
		sink.subscribe(sub2);

		assertThat(sink.tryEmitError(new IllegalStateException("boom"))).as("tryEmitError").isEqualTo(Emission.OK);

		sub1.assertErrorMessage("boom");
		sub2.assertErrorMessage("boom");
	}

	@Test
	void addSubscriberThatCancelsOnSubscription() {
		SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();

		sink.subscribe(new BaseSubscriber<Integer>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				subscription.cancel();
			}
		});
	}

	@Nested
	class RaceConditionsAndLoops {

		static final int ROUNDS = 1000;

		@Test
		void parallelSubscribes() {
			ExecutorService service = Executors.newFixedThreadPool(10);

			try {
				for (int i = 0; i < ROUNDS; i++) {
					SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();

					CountDownLatch latch = new CountDownLatch(10);
					for (int sub = 0; sub < 10; sub++) {
						service.execute(() -> {
							sink.subscribe(AssertSubscriber.create());
							latch.countDown();
						});
					}

					assertThat(latch.await(1, TimeUnit.SECONDS)).as("1s timeout").isTrue();
					assertThat(sink.currentSubscriberCount()).isEqualTo(10);
				}
			}
			catch (InterruptedException e) {
				fail("latch interrupted", e);
			}
			finally {
				service.shutdownNow();
			}
		}

		@Test
		void loopCancelOnSubscribeOneSubscriber() {
			for (int i = 0; i < ROUNDS; i++) {
				SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();
				BaseSubscriber<Integer> sub = new BaseSubscriber<Integer>() {
					@Override
					protected void hookOnSubscribe(Subscription subscription) {
						subscription.cancel();
					}
				};

				sink.subscribe(sub);

				assertThat(sink.currentSubscriberCount()).isZero();
			}
		}

		@Test
		void loopCancelOnSubscribeTwoSubscribers() {
			for (int i = 0; i < ROUNDS; i++) {
				SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();
				AssertSubscriber<Integer> sub1 = AssertSubscriber.create(0);
				BaseSubscriber<Integer> sub2 = new BaseSubscriber<Integer>() {
					@Override
					protected void hookOnSubscribe(Subscription subscription) {
						subscription.cancel();
					}
				};

				sink.subscribe(sub1);
				sink.subscribe(sub2);

				assertThat(sink.currentSubscriberCount()).isOne();
			}
		}

		@Test
		void loopCancelledBeforeSubscribeOneSubscriber() {
			for (int i = 0; i < ROUNDS; i++) {
				SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();
				AssertSubscriber<Integer> sub = AssertSubscriber.create(0);
				sub.cancel();

				sink.subscribe(sub);

				assertThat(sink.currentSubscriberCount()).isZero();
			}
		}

		@Test
		void loopCancelledBeforeSubscribeTwoSubscribers() {
			for (int i = 0; i < ROUNDS; i++) {
				SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();
				AssertSubscriber<Integer> sub1 = AssertSubscriber.create(0);
				AssertSubscriber<Integer> sub2 = AssertSubscriber.create(0);
				sub2.cancel();

				sink.subscribe(sub1);
				sink.subscribe(sub2);

				assertThat(sink.currentSubscriberCount()).isOne();
			}
		}

	}

	@Nested
	class IndividualSpecific {

		@Test
		void downstreamStillUsableAfterBackpressure() {
			SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();

			AssertSubscriber<Integer> sub1 = AssertSubscriber.create();
			AssertSubscriber<Integer> sub2 = AssertSubscriber.create(0);

			sink.subscribe(sub1);
			sink.subscribe(sub2);

			assertThat(sink.tryEmitNext(1)).as("tryEmitNext(1)").isEqualTo(Emission.FAIL_OVERFLOW);

			sub1.assertValues(1).assertNotTerminated();
			sub2.assertNoValues().assertNotTerminated();

			sub2.request(1);

			assertThat(sink.tryEmitNext(2)).as("tryEmitNext(2)").isEqualTo(Emission.OK);
			sub1.assertValues(1, 2);
			sub2.assertValues(2);

			sink.tryEmitComplete().orThrow();
			sub1.assertValues(1, 2).assertComplete();
			sub2.assertValues(2).assertComplete();
		}
	}

	@Nested
	class AllOrNothingSpecific {

		@Test
		void downstreamStillUsableAfterBackpressure() {
			SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createAllOrNothing();

			AssertSubscriber<Integer> sub1 = AssertSubscriber.create();
			AssertSubscriber<Integer> sub2 = AssertSubscriber.create(0);

			sink.subscribe(sub1);
			sink.subscribe(sub2);

			assertThat(sink.tryEmitNext(1)).as("tryEmitNext(1)").isEqualTo(Emission.FAIL_OVERFLOW);

			sub1.assertNoValues().assertNotTerminated();
			sub2.assertNoValues().assertNotTerminated();

			sub2.request(1);

			assertThat(sink.tryEmitNext(2)).as("tryEmitNext(2)").isEqualTo(Emission.OK);
			sub1.assertValues(2);
			sub2.assertValues(2);

			sink.tryEmitComplete().orThrow();
			sub1.assertValues(2).assertComplete();
			sub2.assertValues(2).assertComplete();
		}

		@Test
		void oneCancelledDuringRequestAccountingLoopIsIgnored() {
			SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createAllOrNothing();

			AssertSubscriber<Integer> sub1 = AssertSubscriber.create();
			AssertSubscriber<Integer> sub2 = AssertSubscriber.create(0);

			sink.subscribe(sub1);
			sink.subscribe(sub2);

			SinkManyBestEffort.Inner<Integer> inner2 = sink.subscribers[1];
			inner2.set(true);

			assertThat(sink.tryEmitNext(1)).as("tryEmitNext(1)").isEqualTo(Emission.OK);

			sub1.assertValues(1).assertNotTerminated();
			sub2.assertNoValues().assertNotTerminated();

			//now properly remove / cancel the subscriber
			sink.remove(inner2);

			assertThat(sink.tryEmitNext(2)).as("tryEmitNext(2)").isEqualTo(Emission.OK);
			assertThat(sink.tryEmitComplete()).as("tryEmitComplete").isEqualTo(Emission.OK);
			sub1.assertValues(1, 2).assertComplete();
			sub2.assertNoValues().assertNotTerminated();
		}

		@Test
		void allCancelledDuringRequestAccountingLoopIsIgnored() {
			SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createAllOrNothing();

			AssertSubscriber<Integer> sub1 = AssertSubscriber.create();
			AssertSubscriber<Integer> sub2 = AssertSubscriber.create(0);

			sink.subscribe(sub1);
			sink.subscribe(sub2);

			sink.subscribers[0].set(true);
			sink.subscribers[1].set(true);

			assertThat(sink.tryEmitNext(1)).as("tryEmitNext(1)").isEqualTo(Emission.FAIL_ZERO_SUBSCRIBER);

			sub1.assertNoValues().assertNotTerminated();
			sub2.assertNoValues().assertNotTerminated();
		}
	}
}