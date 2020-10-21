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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.SinkManyBestEffort.DirectInner;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

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

		assertThat(sink.tryEmitNext(1)).as("tryEmitNext").isEqualTo(Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER);
	}

	@Test
	void tryEmitNextOnTerminated() {
		SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();
		sink.tryEmitComplete().orThrow();

		assertThat(sink.tryEmitNext(1)).as("tryEmitNext").isEqualTo(Sinks.EmitResult.FAIL_TERMINATED);
	}

	@Test
	void tryEmitCompleteNoSubscribersRetains() {
		SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();

		assertThat(sink.tryEmitComplete()).as("tryEmitComplete").isEqualTo(EmitResult.OK);

		StepVerifier.create(sink.asFlux())
		            .verifyComplete();
	}

	@Test
	void tryEmitCompleteOnTerminated() {
		SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();
		sink.tryEmitComplete().orThrow();

		assertThat(sink.tryEmitComplete()).as("tryEmitComplete").isEqualTo(EmitResult.FAIL_TERMINATED);
	}

	@Test
	void tryEmitErrorNoSubscribersRetains() {
		SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();

		assertThat(sink.tryEmitError(new IllegalStateException("boom"))).as("tryEmitError").isEqualTo(
				Sinks.EmitResult.OK);

		StepVerifier.create(sink.asFlux())
		            .verifyErrorMessage("boom");
	}

	@Test
	void tryEmitErrorOnTerminated() {
		SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();
		sink.tryEmitError(new IllegalStateException("boom")).orThrow();

		assertThat(sink.tryEmitError(new IllegalStateException("boom ignored"))).as("tryEmitError").isEqualTo(
				Sinks.EmitResult.FAIL_TERMINATED);

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

		assertThat(sink.tryEmitError(new IllegalStateException("boom"))).as("tryEmitError").isEqualTo(
				Sinks.EmitResult.OK);

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

		assertThat(sink.currentSubscriberCount()).as("immediately removes").isZero();
	}

	@Test
	void innersReflectsSubscribers() {
		SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();

		assertThat(sink.inners()).as("before subscribers").isEmpty();

		Disposable sub1 = sink.subscribe();
		Disposable sub2 = sink.subscribe();

		assertThat(sink.inners()).hasSize(2);
		assertThat(sink.inners().map(inner -> inner.scanUnsafe(Scannable.Attr.ACTUAL)))
				.containsExactly(sub1, sub2);
	}

	@Test
	void scanSink() {
		SinkManyBestEffort<Integer> sinkNormal = SinkManyBestEffort.createBestEffort();

		assertThat(sinkNormal.scan(Scannable.Attr.TERMINATED)).as("normal not terminated").isFalse();
		sinkNormal.tryEmitComplete().orThrow();
		assertThat(sinkNormal.scan(Scannable.Attr.TERMINATED)).as("normal terminated").isTrue();

		SinkManyBestEffort<Integer> sinkError = SinkManyBestEffort.createBestEffort();
		Throwable expectedError = new IllegalStateException("boom");
		sinkError.tryEmitError(expectedError).orThrow();

		assertThat(sinkError.scan(Scannable.Attr.TERMINATED)).as("error terminated").isTrue();
		assertThat(sinkError.scan(Scannable.Attr.ERROR)).as("error captured").isSameAs(expectedError);
	}

	@Test
	void scanInner() {
		@SuppressWarnings("unchecked")
		InnerConsumer<? super String> actual = mock(InnerConsumer.class);
		@SuppressWarnings("unchecked")
		DirectInnerContainer<String> parent = mock(DirectInnerContainer.class);

		DirectInner<String> test = new SinkManyBestEffort.DirectInner<>(actual, parent);

		assertThat(test.scanUnsafe(Scannable.Attr.PARENT)).isSameAs(parent); //the mock isn't scannable
		assertThat(test.scanUnsafe(Scannable.Attr.ACTUAL)).isSameAs(actual); //the mock isn't scannable
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Nested
	class BestEffortSpecific {

		@Test
		void downstreamStillUsableAfterBackpressure() {
			SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();

			AssertSubscriber<Integer> sub1 = AssertSubscriber.create();
			AssertSubscriber<Integer> sub2 = AssertSubscriber.create(0);

			sink.subscribe(sub1);
			sink.subscribe(sub2);

			assertThat(sink.tryEmitNext(1)).as("tryEmitNext(1)").isEqualTo(EmitResult.OK);

			sub1.assertValues(1).assertNotTerminated();
			sub2.assertNoValues().assertNotTerminated(); //for best effort, represents backpressure (drop)

			sub2.request(1);

			assertThat(sink.tryEmitNext(2)).as("tryEmitNext(2)").isEqualTo(Sinks.EmitResult.OK);
			sub1.assertValues(1, 2);
			sub2.assertValues(2);

			sink.tryEmitComplete().orThrow();
			sub1.assertValues(1, 2).assertComplete();
			sub2.assertValues(2).assertComplete();
		}

		@Test
		void allCancelledWhenTryingEmitInner() {
			SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();

			AssertSubscriber<Integer> sub1 = AssertSubscriber.create();
			AssertSubscriber<Integer> sub2 = AssertSubscriber.create(0);

			sink.subscribe(sub1);
			sink.subscribe(sub2);

			sink.subscribers[0].set(true); //mark as cancelled
			sink.subscribers[1].set(true);

			assertThat(sink.tryEmitNext(1)).as("tryEmitNext(1)").isEqualTo(EmitResult.FAIL_ZERO_SUBSCRIBER);

			sub1.assertNoValues().assertNotTerminated();
			sub2.assertNoValues().assertNotTerminated();
		}

		@Test
		void halfReceivedWhenSlowSubscriber() {
			SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createBestEffort();

			AssertSubscriber<Integer> sub1 = AssertSubscriber.create();
			AssertSubscriber<Integer> sub2 = AssertSubscriber.create(0);

			sink.subscribe(sub1);
			sink.subscribe(sub2);

			assertThat(sink.tryEmitNext(1)).as("tryEmitNext(1)").isEqualTo(EmitResult.OK);

			sub1.assertValues(1).assertNotTerminated();
			sub2.assertNoValues().assertNotTerminated();

			sink.tryEmitComplete().orThrow();
			sub1.assertValues(1).assertComplete();
			sub2.assertNoValues().assertComplete();
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

			assertThat(sink.tryEmitNext(1)).as("tryEmitNext(1)").isEqualTo(EmitResult.FAIL_OVERFLOW);

			sub1.assertNoValues().assertNotTerminated();
			sub2.assertNoValues().assertNotTerminated();

			sub2.request(1);

			assertThat(sink.tryEmitNext(2)).as("tryEmitNext(2)").isEqualTo(Sinks.EmitResult.OK);
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

			SinkManyBestEffort.DirectInner<Integer> inner2 = sink.subscribers[1];
			inner2.set(true);

			assertThat(sink.tryEmitNext(1)).as("tryEmitNext(1)").isEqualTo(Sinks.EmitResult.OK);

			sub1.assertValues(1).assertNotTerminated();
			sub2.assertNoValues().assertNotTerminated();

			//now properly remove / cancel the subscriber
			sink.remove(inner2);

			assertThat(sink.tryEmitNext(2)).as("tryEmitNext(2)").isEqualTo(Sinks.EmitResult.OK);
			assertThat(sink.tryEmitComplete()).as("tryEmitComplete").isEqualTo(EmitResult.OK);
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

			assertThat(sink.tryEmitNext(1)).as("tryEmitNext(1)").isEqualTo(Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER);

			sub1.assertNoValues().assertNotTerminated();
			sub2.assertNoValues().assertNotTerminated();
		}

		@Test
		void noneReceivedWhenSlowSubscriber() {
			SinkManyBestEffort<Integer> sink = SinkManyBestEffort.createAllOrNothing();

			AssertSubscriber<Integer> sub1 = AssertSubscriber.create();
			AssertSubscriber<Integer> sub2 = AssertSubscriber.create(0);

			sink.subscribe(sub1);
			sink.subscribe(sub2);

			assertThat(sink.tryEmitNext(1)).as("tryEmitNext(1)").isEqualTo(EmitResult.FAIL_OVERFLOW);

			sub1.assertNoValues().assertNotTerminated();
			sub2.assertNoValues().assertNotTerminated();

			sink.tryEmitComplete().orThrow();
			sub1.assertNoValues().assertComplete();
			sub2.assertNoValues().assertComplete();
		}
	}
}