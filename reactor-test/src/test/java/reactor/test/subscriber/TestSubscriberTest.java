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

package reactor.test.subscriber;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.reactivestreams.Subscription;

import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Signal;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;

/**
 * @author Simon Baslé
 */
class TestSubscriberTest {

	@Test
	void requestFailsIfNotSubscribed() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();

		assertThatIllegalStateException().isThrownBy(() -> testSubscriber.request(1))
				.withMessage("Request can only happen once a Subscription has been established." +
						"Have you subscribed the TestSubscriber?");
	}

	@Test
	void cancelBeforeSubscriptionSetAppliesLazily() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		testSubscriber.cancel();

		AtomicBoolean cancelled = new AtomicBoolean();
		Subscription s = Mockito.mock(Subscription.class);
		Mockito.doAnswer((Answer<Object>) invocation -> {
			cancelled.set(true);
			return null;
		}).when(s).cancel();

		assertThat(cancelled).as("pre subscription").isFalse();

		testSubscriber.onSubscribe(s);

		assertThat(cancelled).as("post subscription").isTrue();
	}

	@Test
	void requestFusionSync() {
		Flux<Integer> source = Flux.range(1, 10);
		TestSubscriber<Integer> testSubscriber = TestSubscriber.builder()
				.requireFusion(Fuseable.SYNC)
				.build();

		source.subscribe(testSubscriber);

		testSubscriber.block();

		assertThat(Fuseable.fusionModeName(testSubscriber.getFusionMode()))
				.as("fusion mode")
				.isEqualTo(Fuseable.fusionModeName(Fuseable.SYNC));

		assertThat(testSubscriber.getProtocolErrors()).as("behaved normally").isEmpty();
		assertThat(testSubscriber.getReceivedOnNext()).as("onNext").containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		assertThat(testSubscriber.getTerminalSignal()).matches(Signal::isOnComplete, "completed");
	}

	@Test
	void requestFusionSyncButGetNormal() {
		final TestSubscriber<Object> subscriber = TestSubscriber.builder().requireFusion(Fuseable.SYNC).build();

		assertThatCode(() -> subscriber.onSubscribe(Mockito.mock(Subscription.class)))
				.doesNotThrowAnyException();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(subscriber::block)
				.isSameAs(subscriber.getFailure())
				.withMessageStartingWith("TestSubscriber configured to require QueueSubscription, got Mock for Subscription");
	}

	@Test
	void requestFusionSyncButGetOtherFusion() {
		final Fuseable.QueueSubscription<?> mock = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(mock.requestFusion(anyInt())).thenReturn(Fuseable.ASYNC);

		final TestSubscriber<Object> subscriber = TestSubscriber.builder().requireFusion(Fuseable.SYNC).build();

		assertThatCode(() -> subscriber.onSubscribe(mock))
				.doesNotThrowAnyException();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(subscriber::block)
				.isSameAs(subscriber.getFailure())
				.withMessage("TestSubscriber negotiated fusion mode inconsistent, expected SYNC got ASYNC");
	}

	@Test
	void syncFusionModeDisallowsRequest() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.builder()
				.requireFusion(Fuseable.SYNC)
				.build();
		Flux.range(1, 10).subscribe(testSubscriber);

		assertThat(testSubscriber.getFusionMode()).as("fusion mode").isEqualTo(Fuseable.SYNC);

		assertThatIllegalStateException().isThrownBy(() -> testSubscriber.request(1))
				.withMessage("Request is short circuited in SYNC fusion mode, and should not be explicitly used");
	}

	@Test
	void requestFusionAsync() {
		Flux<Integer> source = Flux.range(1, 10)
				.publishOn(Schedulers.immediate());

		TestSubscriber<Integer> testSubscriber = TestSubscriber.builder()
				.requireFusion(Fuseable.ASYNC)
				.build();

		source.subscribe(testSubscriber);

		testSubscriber.block();

		assertThat(Fuseable.fusionModeName(testSubscriber.getFusionMode()))
				.as("fusion mode")
				.isEqualTo(Fuseable.fusionModeName(Fuseable.ASYNC));

		assertThat(testSubscriber.getProtocolErrors()).as("behaved normally").isEmpty();
		assertThat(testSubscriber.getReceivedOnNext()).as("onNext").containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		assertThat(testSubscriber.getTerminalSignal()).matches(Signal::isOnComplete, "completed");
	}

	@Test
	void requestFusionAsyncButGetNormal() {
		final TestSubscriber<Object> subscriber = TestSubscriber.builder().requireFusion(Fuseable.ASYNC).build();

		assertThatCode(() -> subscriber.onSubscribe(Mockito.mock(Subscription.class)))
				.doesNotThrowAnyException();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(subscriber::block)
				.isSameAs(subscriber.getFailure())
				.withMessageStartingWith("TestSubscriber configured to require QueueSubscription, got Mock for Subscription");
	}

	@Test
	void requestFusionAsyncButGetOtherFusion() {
		final Fuseable.QueueSubscription<?> mock = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(mock.requestFusion(anyInt())).thenReturn(Fuseable.SYNC | Fuseable.THREAD_BARRIER);

		final TestSubscriber<Object> subscriber = TestSubscriber.builder().requireFusion(Fuseable.ASYNC).build();

		assertThatCode(() -> subscriber.onSubscribe(mock))
				.doesNotThrowAnyException();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(subscriber::block)
				.isSameAs(subscriber.getFailure())
				.withMessage("TestSubscriber negotiated fusion mode inconsistent, expected ASYNC got SYNC+THREAD_BARRIER");
	}

	@Test
	void requestFusionAnyAndExpectSync_getSync() {
		final Fuseable.QueueSubscription<?> mock = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(mock.requestFusion(anyInt())).thenReturn(Fuseable.SYNC);

		final TestSubscriber<Object> subscriber = TestSubscriber.builder()
				.requireFusion(Fuseable.ANY, Fuseable.SYNC)
				.build();

		subscriber.onSubscribe(mock);

		assertThat(Fuseable.fusionModeName(subscriber.getFusionMode()))
				.isEqualTo(Fuseable.fusionModeName(Fuseable.SYNC));
	}

	@Test
	void requestFusionAnyAndExpectSync_getOther() {
		final Fuseable.QueueSubscription<?> mock = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(mock.requestFusion(anyInt())).thenReturn(Fuseable.NONE);

		final TestSubscriber<Object> subscriber = TestSubscriber.builder()
				.requireFusion(Fuseable.ANY, Fuseable.SYNC)
				.build();

		assertThatCode(() -> subscriber.onSubscribe(mock))
				.doesNotThrowAnyException();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(subscriber::block)
				.isSameAs(subscriber.getFailure())
				.withMessage("TestSubscriber negotiated fusion mode inconsistent, expected SYNC got NONE");
	}

	@Test
	void requestFusionAnyAndExpectAsync_getAsync() {
		final Fuseable.QueueSubscription<?> mock = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(mock.requestFusion(anyInt())).thenReturn(Fuseable.ASYNC);

		final TestSubscriber<Object> subscriber = TestSubscriber.builder()
				.requireFusion(Fuseable.ANY, Fuseable.ASYNC)
				.build();

		subscriber.onSubscribe(mock);

		assertThat(Fuseable.fusionModeName(subscriber.getFusionMode()))
				.isEqualTo(Fuseable.fusionModeName(Fuseable.ASYNC));
	}

	@Test
	void requestFusionAnyAndExpectAsync_getOther() {
		final Fuseable.QueueSubscription<?> mock = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(mock.requestFusion(anyInt())).thenReturn(Fuseable.NONE);

		final TestSubscriber<Object> subscriber = TestSubscriber.builder()
				.requireFusion(Fuseable.ANY, Fuseable.ASYNC)
				.build();

		assertThatCode(() -> subscriber.onSubscribe(mock))
				.doesNotThrowAnyException();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(subscriber::block)
				.isSameAs(subscriber.getFailure())
				.withMessage("TestSubscriber negotiated fusion mode inconsistent, expected ASYNC got NONE");
	}

	@Test
	void requestFusionAnyIntrospectionOfMode() {
		final Fuseable.QueueSubscription<?> mock = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(mock.requestFusion(anyInt())).thenReturn(Fuseable.ASYNC);

		final TestSubscriber<Object> subscriber = TestSubscriber.builder()
				.requireFusion(Fuseable.ANY, Fuseable.ANY)
				.build();

		subscriber.onSubscribe(mock);

		assertThat(Fuseable.fusionModeName(subscriber.getFusionMode()))
				.isEqualTo(Fuseable.fusionModeName(Fuseable.ASYNC));
	}

	@Test
	void syncPollInterruptedByCancel() {
		AtomicInteger source = new AtomicInteger();

		final TestSubscriber<Object> subscriber = TestSubscriber.builder()
				.requireFusion(Fuseable.ANY, Fuseable.SYNC)
				.build();

		@SuppressWarnings("rawtypes")
		final Fuseable.QueueSubscription mock = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(mock.requestFusion(anyInt())).thenReturn(Fuseable.SYNC);
		Mockito.when(mock.poll())
				.thenAnswer(new Answer<Integer>() {
					@Override
					@Nullable
					public Integer answer(InvocationOnMock invocation) {
						int value = source.incrementAndGet();
						if (value == 4) {
							//emulate a precisely concurrent cancellation
							subscriber.cancel();
							return 4;
						}
						if (value == 8) {
							return null;
						}
						return value;
					}
				});

		//this call is "blocking", since SYNC fusion is enabled it will attempt to repeatedly poll the mock
		subscriber.onSubscribe(mock);

		assertThat(subscriber.getReceivedOnNext()).containsExactly(1, 2, 3, 4);
		assertThat(subscriber.getTerminalSignal()).as("terminal signal").isNull();
		assertThat(subscriber.isTerminated()).as("isTerminated").isFalse();
		assertThat(subscriber.isCancelled()).as("isCancelled").isTrue();
		assertThat(subscriber.isTerminatedOrCancelled()).as("isTerminatedOrCancelled").isTrue();
	}

	@Test
	void asyncPollInterruptedByCancel() {
		AtomicInteger source = new AtomicInteger();

		final TestSubscriber<Object> subscriber = TestSubscriber.builder()
				.requireFusion(Fuseable.ANY, Fuseable.ASYNC)
				.build();

		@SuppressWarnings("rawtypes")
		final Fuseable.QueueSubscription mock = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(mock.requestFusion(anyInt())).thenReturn(Fuseable.ASYNC);
		Mockito.when(mock.poll())
				.thenAnswer(new Answer<Integer>() {
					@Override
					@Nullable
					public Integer answer(InvocationOnMock invocation) throws Throwable {
						int value = source.incrementAndGet();
						if (value == 4) {
							//emulate a precisely concurrent cancellation
							subscriber.cancel();
							return 4;
						}
						if (value == 8) {
							return null;
						}
						return value;
					}
				});

		subscriber.onSubscribe(mock);
		//this call triggers the polling
		subscriber.onNext(null);

		assertThat(subscriber.getReceivedOnNext()).containsExactly(1, 2, 3, 4);
		assertThat(subscriber.getTerminalSignal()).as("terminal signal").isNull();
		assertThat(subscriber.isTerminated()).as("isTerminated").isFalse();
		assertThat(subscriber.isCancelled()).as("isCancelled").isTrue();
		assertThat(subscriber.isTerminatedOrCancelled()).as("isTerminatedOrCancelled").isTrue();
	}

	@Test
	void requestNoFusionGotQueueSubscription() {
		final Fuseable.QueueSubscription<?> mock = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(mock.requestFusion(anyInt())).thenReturn(Fuseable.ASYNC);

		final TestSubscriber<Object> subscriber = TestSubscriber.builder()
				.requireNotFuseable()
				.build();

		subscriber.onSubscribe(mock);

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(subscriber::block)
				.isSameAs(subscriber.getFailure())
				.withMessageStartingWith("TestSubscriber configured to reject QueueSubscription, got Mock for QueueSubscription, hashCode: ");



		assertThat(Fuseable.fusionModeName(subscriber.getFusionMode()))
				.isEqualTo("Disabled");
	}

	@Test
	void failFastOnExtraSubscription() {
		final Subscription sub = Mockito.mock(Subscription.class);
		AtomicBoolean subCancelled = new AtomicBoolean();
		Mockito.doAnswer(inv -> { subCancelled.set(true); return null; }).when(sub).cancel();

		final Subscription extraSub = Mockito.mock(Subscription.class);
		AtomicBoolean extraSubCancelled = new AtomicBoolean();
		Mockito.doAnswer(inv -> { extraSubCancelled.set(true); return null; }).when(extraSub).cancel();

		final TestSubscriber<Object> subscriber = TestSubscriber.create();

		subscriber.onSubscribe(sub);
		subscriber.onSubscribe(extraSub);

		assertThat(subscriber.s).isSameAs(sub);
		assertThat(extraSubCancelled).as("extraSub cancelled").isTrue();
		assertThat(subCancelled).as("sub cancelled").isTrue();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(subscriber::block)
				.isSameAs(subscriber.getFailure())
				.withMessage("TestSubscriber must not be reused, but Subscription has already been set.");
	}

	@Test
	void protocolErrorExtraOnNext() {
		final Subscription mock = Mockito.mock(Subscription.class);

		final TestSubscriber<Object> subscriber = TestSubscriber.create();

		subscriber.onSubscribe(mock);
		subscriber.onComplete();

		subscriber.onNext(123);

		assertThat(subscriber.getProtocolErrors()).as("protocol errors")
				.containsExactly(Signal.next(123))
				.allMatch(s -> s.getContextView().isEmpty(), "empty context");
	}

	@Test
	void protocolErrorExtraOnComplete() {
		final Subscription mock = Mockito.mock(Subscription.class);

		final TestSubscriber<Object> subscriber = TestSubscriber.create();

		subscriber.onSubscribe(mock);
		subscriber.onError(new IllegalStateException("boom"));

		subscriber.onComplete();

		assertThat(subscriber.getProtocolErrors()).as("protocol errors")
				.containsExactly(Signal.complete())
				.allMatch(s -> s.getContextView().isEmpty(), "empty context");
	}

	@Test
	void protocolErrorExtraOnError() {
		final Subscription mock = Mockito.mock(Subscription.class);

		final TestSubscriber<Object> subscriber = TestSubscriber.create();

		subscriber.onSubscribe(mock);
		subscriber.onComplete();

		Throwable expectDropped = new IllegalStateException("expected protocol error");
		subscriber.onError(expectDropped);

		assertThat(subscriber.getProtocolErrors()).as("protocol errors")
				.containsExactly(Signal.error(expectDropped))
				.allMatch(s -> s.getContextView().isEmpty(), "empty context");
	}

	@Test
	void onNextNullWhenNoFusion() {
		final Subscription mock = Mockito.mock(Subscription.class);

		final TestSubscriber<Object> subscriber = TestSubscriber.create();

		subscriber.onSubscribe(mock);

		assertThatCode(() -> subscriber.onNext(null))
				.doesNotThrowAnyException();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(subscriber::block)
				.isSameAs(subscriber.getFailure())
				.withMessage("onNext(null) received while ASYNC fusion not established");
	}

	@Test
	void onNextNullWhenSyncFusion() {
		final Fuseable.QueueSubscription<?> mock = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(mock.requestFusion(anyInt())).thenReturn(Fuseable.SYNC);

		final TestSubscriber<Object> subscriber = TestSubscriber.builder()
				.requireFusion(Fuseable.SYNC)
				.build();

		subscriber.onSubscribe(mock);
		//actually at that point the source has been entirely polled so we're terminated

		assertThat(subscriber.isTerminated()).as("isTerminated").isTrue();

		assertThatCode(() -> subscriber.onNext(null))
				.doesNotThrowAnyException();

		assertThat(subscriber.getProtocolErrors()).hasSize(1);
		Signal<Object> expectedErrorSignal = subscriber.getProtocolErrors().get(0);

		assertThat(expectedErrorSignal.getThrowable())
				.isNotNull()
				.hasMessage("onNext(null) received despite SYNC fusion (which has already completed)");
	}

	@Test
	void requestZeroInitiallyThenSmallRequest() {
		Flux<Integer> source = Flux.range(1, 100).hide();
		TestSubscriber<Integer> subscriber = TestSubscriber.builder().initialRequest(0L).build();

		source.subscribe(subscriber);

		assertThat(subscriber.getReceivedOnNext()).as("receivedOnNext before request").isEmpty();

		subscriber.request(3L);

		assertThat(subscriber.getReceivedOnNext())
				.as("receivedOnNext after request")
				.containsExactly(1, 2, 3);
		assertThat(subscriber.isTerminatedOrCancelled()).as("isTerminated/Cancelled after request").isFalse();
	}

	@Test
	void expectTerminalSignal_notTerminatedCancelsThrows() {
		AtomicBoolean cancelled = new AtomicBoolean();
		final Subscription mock = Mockito.mock(Subscription.class);
		Mockito.doAnswer(inv -> {
			cancelled.set(true);
			return null;
		}).when(mock).cancel();

		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		testSubscriber.onSubscribe(mock);

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(testSubscriber::expectTerminalSignal)
				.withMessage("Expected subscriber to be terminated, but it has not been terminated yet.");

		assertThat(cancelled.get()).as("subscription was cancelled").isTrue();
	}

	@Test
	void expectTerminalSignal_unexpectedSignalCancelsThrows() {
		AtomicBoolean cancelled = new AtomicBoolean();
		final Subscription mock = Mockito.mock(Subscription.class);
		Mockito.doAnswer(inv -> {
			cancelled.set(true);
			return null;
		}).when(mock).cancel();

		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		testSubscriber.onSubscribe(mock);

		//force something that shouldn't happen
		testSubscriber.terminalSignal.set(Signal.next(1));

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(testSubscriber::expectTerminalSignal)
				.withMessage("Expected subscriber to be terminated, but it has not been terminated yet.");

		assertThat(cancelled.get()).as("subscription was cancelled").isTrue();
	}

	@Test
	void expectTerminalSignal_completedReturnsSignal() {
		AtomicBoolean cancelled = new AtomicBoolean();
		final Subscription mock = Mockito.mock(Subscription.class);
		Mockito.doAnswer(inv -> {
			cancelled.set(true);
			return null;
		}).when(mock).cancel();

		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		testSubscriber.onSubscribe(mock);
		testSubscriber.onComplete();

		assertThat(testSubscriber.expectTerminalSignal())
				.satisfies(sig -> {
					assertThat(sig.isOnComplete()).as("isOnComplete").isTrue();
					assertThat(sig.getContextView().isEmpty()).as("contextView").isTrue();
				});

		assertThat(cancelled.get()).as("subscription was not cancelled").isFalse();
	}

	@Test
	void expectTerminalSignal_erroredReturnsSignal() {
		AtomicBoolean cancelled = new AtomicBoolean();
		final Subscription mock = Mockito.mock(Subscription.class);
		Mockito.doAnswer(inv -> {
			cancelled.set(true);
			return null;
		}).when(mock).cancel();

		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		testSubscriber.onSubscribe(mock);
		Throwable expected = new IllegalStateException("boom");
		testSubscriber.onError(expected);

		assertThat(testSubscriber.expectTerminalSignal())
				.satisfies(sig -> {
					assertThat(sig.isOnError()).as("isOnError").isTrue();
					assertThat(sig.getThrowable()).as("getThrowable").isEqualTo(expected);
					assertThat(sig.getContextView().isEmpty()).as("contextView").isTrue();
				});

		assertThat(cancelled.get()).as("subscription was not cancelled").isFalse();
	}

	@Test
	void expectTerminalError_notTerminatedCancelsThrows() {
		AtomicBoolean cancelled = new AtomicBoolean();
		final Subscription mock = Mockito.mock(Subscription.class);
		Mockito.doAnswer(inv -> {
			cancelled.set(true);
			return null;
		}).when(mock).cancel();

		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		testSubscriber.onSubscribe(mock);

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(testSubscriber::expectTerminalError)
				.withMessage("Expected subscriber to have errored, but it has not been terminated yet.");

		assertThat(cancelled.get()).as("subscription was cancelled").isTrue();
	}

	@Test
	void expectTerminalError_completedThrows() {
		AtomicBoolean cancelled = new AtomicBoolean();
		final Subscription mock = Mockito.mock(Subscription.class);
		Mockito.doAnswer(inv -> {
			cancelled.set(true);
			return null;
		}).when(mock).cancel();

		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		testSubscriber.onSubscribe(mock);
		testSubscriber.onComplete();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(testSubscriber::expectTerminalError)
				.withMessage("Expected subscriber to have errored, but it has completed instead.");

		assertThat(cancelled.get()).as("subscription was not cancelled").isFalse();
	}

	@Test
	void expectTerminalError_unexpectedSignalCancelsThrows() {
		AtomicBoolean cancelled = new AtomicBoolean();
		final Subscription mock = Mockito.mock(Subscription.class);
		Mockito.doAnswer(inv -> {
			cancelled.set(true);
			return null;
		}).when(mock).cancel();

		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		testSubscriber.onSubscribe(mock);

		//force something that shouldn't happen
		testSubscriber.terminalSignal.set(Signal.next(1));

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(testSubscriber::expectTerminalError)
				.withMessage("Expected subscriber to have errored, got unexpected terminal signal <onNext(1)>.");

		assertThat(cancelled.get()).as("subscription was cancelled").isTrue();
	}

	@Test
	void expectTerminalError_errorReturnsThrowable() {
		AtomicBoolean cancelled = new AtomicBoolean();
		final Subscription mock = Mockito.mock(Subscription.class);
		Mockito.doAnswer(inv -> {
			cancelled.set(true);
			return null;
		}).when(mock).cancel();

		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		testSubscriber.onSubscribe(mock);
		testSubscriber.onError(new IllegalStateException("expected"));

		assertThat(testSubscriber.expectTerminalError())
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("expected");

		assertThat(cancelled.get()).as("subscription was not cancelled").isFalse();
	}
	//TODO block(Duration) tests
	//TODO getOnNextSublist... tests
	//TODO scan
}