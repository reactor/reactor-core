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

package reactor.test.subscriber;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.reactivestreams.Subscription;

import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Signal;
import reactor.core.scheduler.Schedulers;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;

/**
 * @author Simon Baslé
 */
@Timeout(10)
class DefaultTestSubscriberTest {

	private static final Logger LOGGER = Loggers.getLogger(DefaultTestSubscriberTest.class);
	private static final int RACE_DETECTION_LOOPS = 1000;

	@Nullable
	static Throwable captureThrow(Runnable r) {
		try {
			r.run();
			return null;
		}
		catch (Throwable t) {
			return t;
		}
	}

	@Test
	void requestAccumulatesIfNotSubscribed() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.builder().initialRequest(0L).build();
		DefaultTestSubscriber<Integer> underTest = (DefaultTestSubscriber<Integer>) testSubscriber;
		underTest.request(1);

		assertThat(underTest.requestedTotal)
				.as("requestedTotal before subscribe")
				.isZero();
		assertThat(underTest.requestedPreSubscription)
				.as("requestedPreSubscription before subscribe")
				.isEqualTo(1L);

		underTest.onSubscribe(Operators.emptySubscription());

		assertThat(underTest.requestedTotal)
				.as("requestedTotal after subscribe")
				.isEqualTo(1L);
		assertThat(underTest.requestedPreSubscription)
				.as("requestedPreSubscription after subscribe")
				.isEqualTo(-1L);
	}

	@Test
	void requestAccumulatedIfNotSubscribedStartsWithInitialRequest() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.builder().initialRequest(123L).build();
		DefaultTestSubscriber<Integer> underTest = (DefaultTestSubscriber<Integer>) testSubscriber;
		underTest.request(1);

		assertThat(underTest.requestedTotal)
				.as("requestedTotal before subscribe")
				.isZero();
		assertThat(underTest.requestedPreSubscription)
				.as("requestedPreSubscription before subscribe")
				.isEqualTo(124L);

		underTest.onSubscribe(Operators.emptySubscription());

		assertThat(underTest.requestedTotal)
				.as("requestedTotal after subscribe")
				.isEqualTo(124L);
		assertThat(underTest.requestedPreSubscription)
				.as("requestedPreSubscription after subscribe")
				.isEqualTo(-1L);
	}

	@Test
	void cancelBeforeSubscriptionSetAppliesLazily() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		testSubscriber.cancel();

		AtomicBoolean cancelled = new AtomicBoolean();
		Subscription s = Mockito.mock(Subscription.class);
		Mockito.doAnswer(invocation -> {
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
				.isSameAs(captureThrow(subscriber::isTerminatedOrCancelled))
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
				.isSameAs(captureThrow(subscriber::isTerminatedOrCancelled))
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
				.isSameAs(captureThrow(subscriber::isTerminatedOrCancelled))
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
				.isSameAs(captureThrow(subscriber::isTerminatedOrCancelled))
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
				.isSameAs(captureThrow(subscriber::isTerminatedOrCancelled))
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
				.isSameAs(captureThrow(subscriber::isTerminatedOrCancelled))
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
	void requireFusionNoneAcceptsVanillaSubscription() {
		final Subscription mock = Mockito.mock(Subscription.class);

		final DefaultTestSubscriber<Object> subscriber = (DefaultTestSubscriber<Object>) TestSubscriber.builder()
				.requireFusion(Fuseable.NONE)
				.build();

		subscriber.onSubscribe(mock);

		assertThatCode(subscriber::checkSubscriptionFailure).doesNotThrowAnyException();
	}

	@Test
	void requireFusionNoneNoneAcceptsVanillaSubscription() {
		final Subscription mock = Mockito.mock(Subscription.class);

		final DefaultTestSubscriber<Object> subscriber = (DefaultTestSubscriber<Object>) TestSubscriber.builder()
				.requireFusion(Fuseable.NONE, Fuseable.NONE)
				.build();

		subscriber.onSubscribe(mock);

		assertThatCode(subscriber::checkSubscriptionFailure).doesNotThrowAnyException();
	}

	@Test
	void syncPollInterruptedByCancel() {
		AtomicInteger source = new AtomicInteger();

		final DefaultTestSubscriber<Object> subscriber = (DefaultTestSubscriber<Object>) TestSubscriber.builder()
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
		assertThat(subscriber.isTerminatedOrCancelled()).as("isDone").isTrue();
		assertThat(subscriber.getTerminalSignal()).as("terminal signal").isNull();
		assertThat(subscriber.isTerminated()).as("isTerminated").isFalse();
		assertThat(subscriber.isCancelled()).as("isCancelled").isTrue();
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
		assertThat(subscriber.isTerminatedOrCancelled()).as("isDone").isTrue();
		assertThat(subscriber.getTerminalSignal()).as("terminal signal").isNull();
		assertThat(subscriber.isTerminated()).as("isTerminated").isFalse();
		assertThat(subscriber.isCancelled()).as("isCancelled").isTrue();
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
				.isSameAs(captureThrow(subscriber::isTerminatedOrCancelled))
				.withMessageStartingWith("TestSubscriber configured to reject QueueSubscription, got Mock for QueueSubscription, hashCode: ");
	}

	@Test
	void failFastOnExtraSubscription() {
		final Subscription sub = Mockito.mock(Subscription.class);
		AtomicBoolean subCancelled = new AtomicBoolean();
		Mockito.doAnswer(inv -> { subCancelled.set(true); return null; }).when(sub).cancel();

		final Subscription extraSub = Mockito.mock(Subscription.class);
		AtomicBoolean extraSubCancelled = new AtomicBoolean();
		Mockito.doAnswer(inv -> { extraSubCancelled.set(true); return null; }).when(extraSub).cancel();

		final DefaultTestSubscriber<Object> subscriber = new DefaultTestSubscriber<>(new TestSubscriberBuilder());

		subscriber.onSubscribe(sub);
		subscriber.onSubscribe(extraSub);

		assertThat(subscriber.s).isSameAs(sub);
		assertThat(extraSubCancelled).as("extraSub cancelled").isTrue();
		assertThat(subCancelled).as("sub cancelled").isTrue();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(subscriber::block)
				.isSameAs(captureThrow(subscriber::isTerminatedOrCancelled))
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
				.isSameAs(captureThrow(subscriber::isTerminatedOrCancelled))
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
	void onNextNullWhenSyncFusionWithConcurrentOnNext() {
		@SuppressWarnings("unchecked")
		final Fuseable.QueueSubscription<Integer> mock = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(mock.requestFusion(anyInt())).thenReturn(Fuseable.SYNC);

		final TestSubscriber<Integer> subscriberAbstract = TestSubscriber.builder()
				.requireFusion(Fuseable.SYNC)
				.build();
		//massaging the type system to get to the state
		final DefaultTestSubscriber<Integer> subscriber = (DefaultTestSubscriber<Integer>) subscriberAbstract;

		//this is an artificial way of getting in the state where that message occurs,
		//since it is hard to deterministically trigger (it would imply a badly formed
		//publisher wrt fusion but also chance, as it is a race condition)
		subscriber.fusionMode = Fuseable.SYNC;
		subscriber.markOnNextStart();

		assertThatCode(() -> subscriber.onNext(null)).doesNotThrowAnyException();

		assertThat(subscriber.getProtocolErrors()).hasSize(1);
		Signal<Integer> expectedErrorSignal = subscriber.getProtocolErrors().get(0);

		assertThat(expectedErrorSignal.getThrowable())
				.isNotNull()
				.hasMessage("onNext(null) received despite SYNC fusion (with concurrent onNext)");
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
		assertThat(subscriber.isTerminatedOrCancelled()).as("isDone").isFalse();
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
				.withMessage("Expected subscriber to be terminated, but it has not been terminated yet: cancelling subscription.");

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

		DefaultTestSubscriber<Integer> testSubscriber = new DefaultTestSubscriber<>(new TestSubscriberBuilder());
		testSubscriber.onSubscribe(mock);

		//force something that shouldn't happen
		testSubscriber.terminalSignal = Signal.next(1);

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(testSubscriber::expectTerminalSignal)
				.withMessage("Expected subscriber to be terminated, but it has not been terminated yet: cancelling subscription.");

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
		DefaultTestSubscriber<Integer> underTest = (DefaultTestSubscriber<Integer>) testSubscriber;
		underTest.terminalSignal = Signal.next(1);

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

	@Test
	void isTerminatedComplete() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();

		testSubscriber.onComplete();

		assertThat(testSubscriber.isTerminatedOrCancelled()).as("isDone").isTrue();
		assertThat(testSubscriber.isTerminated()).as("isTerminated").isTrue();
		assertThat(testSubscriber.isTerminatedComplete()).as("isTerminatedComplete").isTrue();
		assertThat(testSubscriber.isTerminatedError()).as("isTerminatedError").isFalse();
	}

	@Test
	void isTerminatedError() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();

		testSubscriber.onError(new IllegalStateException("expected"));

		assertThat(testSubscriber.isTerminatedOrCancelled()).as("isDone").isTrue();
		assertThat(testSubscriber.isTerminated()).as("isTerminated").isTrue();
		assertThat(testSubscriber.isTerminatedError()).as("isTerminatedError").isTrue();
		assertThat(testSubscriber.isTerminatedComplete()).as("isTerminatedComplete").isFalse();
	}

	@Test
	void contextAccessibleToSource() {
		AtomicReference<String> keyRead = new AtomicReference<>();
		Mono<Integer> source = Mono.deferContextual(ctx -> {
			keyRead.set(ctx.getOrDefault("example", "NOT_FOUND"));
			return Mono.just(1);
		});

		TestSubscriber<Integer> testSubscriber = TestSubscriber.builder().contextPut("example", "foo").build();

		source.subscribe(testSubscriber);

		assertThat(keyRead).hasValue("foo");
	}

	@Test
	void onNextReceivedAfterCancel() {
		Subscription mock = Mockito.mock(Subscription.class);

		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		testSubscriber.onSubscribe(mock);

		testSubscriber.onNext(1);
		testSubscriber.onNext(2);
		testSubscriber.cancel();
		testSubscriber.onNext(3);

		assertThat(testSubscriber.getReceivedOnNext())
				.as("getReceivedOnNext()")
				.containsExactly(1, 2, 3);
		assertThat(testSubscriber.getReceivedOnNextAfterCancellation())
				.as("getReceivedOnNextAfterCancellation()")
				.containsExactly(3);
	}

	@Test
	void getOnNextReceivedIsACopy() {
		Subscription mock = Mockito.mock(Subscription.class);

		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		testSubscriber.onSubscribe(mock);

		testSubscriber.onNext(1);
		testSubscriber.onNext(2);
		testSubscriber.onNext(3);

		List<Integer> list = testSubscriber.getReceivedOnNext();
		assertThat(list)
				.as("getReceivedOnNext()")
				.containsExactly(1, 2, 3)
				.isNotSameAs(testSubscriber.getReceivedOnNext());

		list.clear();

		assertThat(testSubscriber.getReceivedOnNext())
				.as("after clear()")
				.containsExactly(1, 2, 3);
	}

	@Test
	void getOnNextReceivedAfterCancellationIsACopy() {
		Subscription mock = Mockito.mock(Subscription.class);

		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		testSubscriber.onSubscribe(mock);

		testSubscriber.cancel();
		testSubscriber.onNext(1);
		testSubscriber.onNext(2);
		testSubscriber.onNext(3);

		List<Integer> list = testSubscriber.getReceivedOnNextAfterCancellation();
		assertThat(list)
				.as("getReceivedOnNextAfterCancellation()")
				.containsExactly(1, 2, 3)
				.isNotSameAs(testSubscriber.getReceivedOnNextAfterCancellation());

		list.clear();

		assertThat(testSubscriber.getReceivedOnNextAfterCancellation())
				.as("after clear()")
				.containsExactly(1, 2, 3);
	}

	@Test
	void getOnNextReceivedAfterCancellationIsNotBackedByOtherList() {
		Subscription mock = Mockito.mock(Subscription.class);

		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		testSubscriber.onSubscribe(mock);

		testSubscriber.onNext(1);
		testSubscriber.cancel();
		testSubscriber.onNext(2);
		testSubscriber.onNext(3);

		List<Integer> list = testSubscriber.getReceivedOnNext();
		List<Integer> afterCancelList = testSubscriber.getReceivedOnNextAfterCancellation();


		assertThat(list)
				.as("getReceivedOnNext()")
				.containsExactly(1, 2, 3);
		assertThat(afterCancelList)
				.as("getReceivedOnNextAfterCancellation()")
				.containsExactly(2, 3);

		list.clear();

		assertThat(afterCancelList).as("after clear()").isNotEmpty();
	}

	@Test
	void checkSubscriptionFailureCalledByAllAccessors() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		((DefaultTestSubscriber<Integer>) testSubscriber).subscriptionFail("expected");

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(testSubscriber::isTerminatedOrCancelled)
				.isSameAs(captureThrow(testSubscriber::isTerminated))
				.isSameAs(captureThrow(testSubscriber::isTerminatedComplete))
				.isSameAs(captureThrow(testSubscriber::isTerminatedError))
				.isSameAs(captureThrow(testSubscriber::isCancelled))
				.isSameAs(captureThrow(testSubscriber::getTerminalSignal))
				.isSameAs(captureThrow(testSubscriber::getReceivedOnNext))
				.isSameAs(captureThrow(testSubscriber::getReceivedOnNextAfterCancellation))
				.isSameAs(captureThrow(testSubscriber::getProtocolErrors))
				.isSameAs(captureThrow(testSubscriber::getFusionMode))
				.isSameAs(captureThrow(testSubscriber::block))
				.isSameAs(captureThrow(() -> testSubscriber.block(Duration.ofMillis(100))))
				.isSameAs(captureThrow(testSubscriber::expectTerminalError))
				.isSameAs(captureThrow(testSubscriber::expectTerminalSignal))
				.withMessage("expected");
	}

	@Test
	void scanSubscriber_completed() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();

		//smoke test expected class
		assertThat(testSubscriber).isExactlyInstanceOf(DefaultTestSubscriber.class);

		assertThat(testSubscriber.scan(Scannable.Attr.TERMINATED)).as("TERMINATED before complete").isFalse();

		testSubscriber.onComplete();

		assertThat(testSubscriber.scan(Scannable.Attr.TERMINATED)).as("TERMINATED after complete").isTrue();
		assertThat(testSubscriber.scan(Scannable.Attr.ERROR)).as("ERROR").isNull();
	}

	@Test
	void scanSubscriber_error() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();

		//smoke test expected class
		assertThat(testSubscriber).isExactlyInstanceOf(DefaultTestSubscriber.class);

		assertThat(testSubscriber.scan(Scannable.Attr.TERMINATED)).as("TERMINATED before onError").isFalse();
		assertThat(testSubscriber.scan(Scannable.Attr.ERROR)).as("ERROR before onError").isNull();

		IllegalStateException exception = new IllegalStateException("expected");
		testSubscriber.onError(exception);

		assertThat(testSubscriber.scan(Scannable.Attr.TERMINATED)).as("TERMINATED after onError").isTrue();
		assertThat(testSubscriber.scan(Scannable.Attr.ERROR)).as("ERROR after onError").isSameAs(exception);
	}

	@Test
	void scanSubscriber_subscriptionFailReflected() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();

		//smoke test expected class
		assertThat(testSubscriber).isExactlyInstanceOf(DefaultTestSubscriber.class);

		assertThat(testSubscriber.scan(Scannable.Attr.TERMINATED)).as("TERMINATED before onError").isFalse();
		assertThat(testSubscriber.scan(Scannable.Attr.ERROR)).as("ERROR before onError").isNull();

		((DefaultTestSubscriber<Integer>) testSubscriber).subscriptionFail("expected");

		assertThat(testSubscriber.scan(Scannable.Attr.TERMINATED)).as("TERMINATED after subscriptionFail").isTrue();
		assertThat(testSubscriber.scan(Scannable.Attr.ERROR))
				.as("ERROR after subscriptionFail")
				.isInstanceOf(AssertionError.class)
				.hasMessage("expected");
	}

	@Test
	void scanSubscriber_cancelled() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();

		//smoke test expected class
		assertThat(testSubscriber).isExactlyInstanceOf(DefaultTestSubscriber.class);

		assertThat(testSubscriber.scan(Scannable.Attr.CANCELLED)).as("CANCELLED before").isFalse();

		testSubscriber.cancel();

		assertThat(testSubscriber.scan(Scannable.Attr.CANCELLED)).as("CANCELLED after").isTrue();
		assertThat(testSubscriber.scan(Scannable.Attr.TERMINATED)).as("TERMINATED").isFalse();
	}

	@Test
	void scanSubscriber_misc() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.builder()
				.initialRequest(123L)
				.build();

		//smoke test expected class
		assertThat(testSubscriber).isExactlyInstanceOf(DefaultTestSubscriber.class);

		assertThat(testSubscriber.scan(Scannable.Attr.RUN_STYLE)).as("RUN_STYLE").isEqualTo(Scannable.Attr.RunStyle.SYNC);
		assertThat(testSubscriber.scan(Scannable.Attr.PARENT))
				.as("PARENT before subscription")
				.isNull();

		assertThat(testSubscriber.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM))
				.as("REQUESTED_FROM_DOWNSTREAM before subscribe")
				.isZero();

		Flux.just(1).subscribe(testSubscriber);

		assertThat(testSubscriber.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM))
				.as("REQUESTED_FROM_DOWNSTREAM after subscribe")
				.isEqualTo(123L);

		assertThat(testSubscriber.scan(Scannable.Attr.PARENT))
				.as("PARENT after subscription")
				.isNotNull()
				.matches(Scannable::isScanAvailable, "is Scannable")
				.extracting(Scannable::stepName)
				.isEqualTo("just");

		//just triggering the default path
		assertThat(testSubscriber.scan(Scannable.Attr.ACTUAL_METADATA)).as("ACTUAL_METADATA default")
				.isEqualTo(Scannable.Attr.ACTUAL_METADATA.defaultValue());
	}

	@Test
	void blockWithTimeout() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> testSubscriber.block(Duration.ofMillis(100)))
				.withMessage("TestSubscriber timed out, not terminated after PT0.1S (100ms)");
	}

	@Test
	void blockCanBeInterrupted() throws InterruptedException {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		final AtomicReference<Throwable> errorRef = new AtomicReference<>();

		Thread t = new Thread(() -> {
			try {
				testSubscriber.block();
			}
			catch (Throwable error) {
				errorRef.set(error);
			}
		});

		t.start();
		t.interrupt();

		Thread.sleep(100);
		assertThat(errorRef.get())
				.isInstanceOf(AssertionError.class)
				.hasCauseInstanceOf(InterruptedException.class)
				.hasMessage("Block() interrupted");
	}

	@Test
	void blockWithDurationCanBeInterrupted() throws InterruptedException {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		final AtomicReference<Throwable> errorRef = new AtomicReference<>();

		Thread t = new Thread(() -> {
			try {
				testSubscriber.block(Duration.ofSeconds(5));
			}
			catch (Throwable error) {
				errorRef.set(error);
			}
		});

		t.start();
		t.interrupt();

		Thread.sleep(100);
		assertThat(errorRef.get())
				.isInstanceOf(AssertionError.class)
				.hasCauseInstanceOf(InterruptedException.class)
				.hasMessage("Block(PT5S) interrupted");
	}

	@Test
	void raceDetectionOnNextOnNext() {
		int interesting = 0;
		for (int i = 0; i < RACE_DETECTION_LOOPS; i++) {

			TestPublisher<Integer> badSource = TestPublisher.create();
			TestSubscriber<Integer> testSubscriber = TestSubscriber.create();

			badSource.subscribe(testSubscriber);

			RaceTestUtils.race(
					() -> badSource.next(1),
					() -> badSource.next(2)
			);

			assertThat(testSubscriber.getReceivedOnNext()).hasSizeBetween(1, 2);

			if (testSubscriber.getReceivedOnNext().size() == 1) {
				assertThat(testSubscriber.getProtocolErrors())
						.containsAnyOf(Signal.next(1), Signal.next(2));
			}
			else {
				interesting++;
			}
		}
		LOGGER.info("raceDetectionOnNextOnNext interesting {} / {}", interesting, RACE_DETECTION_LOOPS);
		assertThat(interesting).as("asserts racy cases").isLessThanOrEqualTo(RACE_DETECTION_LOOPS);
	}

	@Test
	void raceDetectionOnNextOnComplete() {
		int interesting = 0;
		//this one has been observed to produce the "interesting" results more often when only doing 100 loops, so we increase to 1000
		final int loops = Math.max(RACE_DETECTION_LOOPS, 1000);
		for (int i = 0; i < loops; i++) {
			TestPublisher<Integer> badSource = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
			TestSubscriber<Integer> testSubscriber = TestSubscriber.create();

			badSource.subscribe(testSubscriber);

			RaceTestUtils.race(
					badSource::complete,
					() -> badSource.next(1)
			);

			List<Signal<Integer>> protocolErrors = testSubscriber.getProtocolErrors();
			if (protocolErrors.isEmpty()) {
				interesting++;
				assertThat(testSubscriber.getReceivedOnNext())
						.as("no protocol errors - onNext")
						.containsExactly(1);
				assertThat(testSubscriber.getTerminalSignal())
						.as("no protocol errors - terminal")
						.isEqualTo(Signal.complete());
			}
			else {
				assertThat(testSubscriber.getProtocolErrors())
						.as("protocol errors")
						.containsAnyOf(Signal.next(1), Signal.complete());
				if (testSubscriber.getProtocolErrors().get(0).isOnNext()) {
					assertThat(testSubscriber.getTerminalSignal())
							.as("protocol error isOnNext - terminal")
							.isEqualTo(Signal.complete());
				}
			}
		}
		LOGGER.info("raceDetectionOnNextOnComplete interesting {} / {}", interesting, loops);
		assertThat(interesting).as("asserts racy cases").isLessThanOrEqualTo(loops);
	}

	@Test
	void raceDetectionOnNextOnError() {
		int interesting = 0;
		for (int i = 0; i < RACE_DETECTION_LOOPS; i++) {
			TestPublisher<Integer> badSource = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
			TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
			IllegalStateException racingException = new IllegalStateException("expected");
			Signal<Integer> racingExceptionSignal = Signal.error(racingException);

			badSource.subscribe(testSubscriber);

			RaceTestUtils.race(
					() -> badSource.error(racingException),
					() -> badSource.next(1)
			);

			List<Signal<Integer>> protocolErrors = testSubscriber.getProtocolErrors();
			if (protocolErrors.isEmpty()) {
				interesting++;
				assertThat(testSubscriber.getReceivedOnNext())
						.as("no protocol errors - onNext")
						.containsExactly(1);
				assertThat(testSubscriber.getTerminalSignal())
						.as("no protocol errors - terminal")
						.isEqualTo(racingExceptionSignal);
			}
			else {
				assertThat(testSubscriber.getProtocolErrors())
						.as("protocol errors")
						.containsAnyOf(Signal.next(1), racingExceptionSignal);
				if (testSubscriber.getProtocolErrors().get(0).isOnNext()) {
					assertThat(testSubscriber.getTerminalSignal())
							.as("protocol error isOnNext - terminal")
							.isEqualTo(racingExceptionSignal);
				}
			}
		}
		LOGGER.info("raceDetectionOnNextOnError interesting {} / {}", interesting, RACE_DETECTION_LOOPS);
		assertThat(interesting).as("asserts racy cases").isLessThanOrEqualTo(RACE_DETECTION_LOOPS);
	}

	@RepeatedTest(100)
	void raceDetectionOnCompleteOnError() {
		TestPublisher<Integer> badSource = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		IllegalStateException racingException = new IllegalStateException("expected");
		Signal<Integer> racingExceptionSignal = Signal.error(racingException);

		badSource.subscribe(testSubscriber);

		RaceTestUtils.race(
				() -> badSource.error(racingException),
				badSource::complete
		);

		//ensure there was a terminal signal
		Signal<Integer> terminalSignal = testSubscriber.expectTerminalSignal();

		if (terminalSignal.equals(Signal.complete())) {
			assertThat(testSubscriber.getProtocolErrors())
					.as("protocol error if terminalSignal onComplete")
					.containsExactly(racingExceptionSignal);
		}
		else if (terminalSignal.equals(racingExceptionSignal)) {
			assertThat(testSubscriber.getProtocolErrors())
					.as("protocol error if terminalSignal onError")
					.containsExactly(Signal.complete());
		}
		else {
			fail("unexpected terminal signal " + terminalSignal);
		}

		assertThat(testSubscriber.getProtocolErrors())
				.containsAnyOf(Signal.complete(), racingExceptionSignal);
	}

	@RepeatedTest(100)
	void raceDetectionOnCompleteOnComplete() {
		TestPublisher<Integer> badSource = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();

		badSource.subscribe(testSubscriber);

		RaceTestUtils.race(
				badSource::complete,
				badSource::complete
		);

		//ensure there was a terminal signal
		assertThat(testSubscriber.getTerminalSignal()).as("terminal signal").isEqualTo(Signal.complete());
		assertThat(testSubscriber.getProtocolErrors())
				.as("second complete in protocol error")
				.containsExactly(Signal.complete());
	}

	@RepeatedTest(100)
	void raceDetectionOnErrorOnError() {
		TestPublisher<Integer> badSource = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
		IllegalStateException racingException1 = new IllegalStateException("expected 1");
		Signal<Integer> racingExceptionSignal1 = Signal.error(racingException1);
		IllegalStateException racingException2 = new IllegalStateException("expected 2");
		Signal<Integer> racingExceptionSignal2 = Signal.error(racingException2);

		badSource.subscribe(testSubscriber);

		RaceTestUtils.race(
				() -> badSource.error(racingException1),
				() -> badSource.error(racingException2)
		);

		//ensure there was a terminal signal
		Signal<Integer> terminalSignal = testSubscriber.expectTerminalSignal();

		if (terminalSignal.equals(racingExceptionSignal1)) {
			assertThat(testSubscriber.getProtocolErrors())
					.as("protocol error if terminalSignal 1")
					.containsExactly(racingExceptionSignal2);
		}
		else if (terminalSignal.equals(racingExceptionSignal2)) {
			assertThat(testSubscriber.getProtocolErrors())
					.as("protocol error if terminalSignal 2")
					.containsExactly(racingExceptionSignal1);
		}
		else {
			fail("unexpected terminal signal " + terminalSignal);
		}
	}

	@Test
	void asyncFusedPrefetchingUpstreamDoesntOverflow() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.builder()
				.requireFusion(Fuseable.ASYNC)
				.initialRequest(3)
				.build();

		Flux.range(1, 10)
				.hide()
				.publishOn(Schedulers.immediate(), 5)
				.subscribe(testSubscriber);

		assertThat(testSubscriber.getReceivedOnNext()).hasSize(3);

		testSubscriber.request(3);
		assertThat(testSubscriber.getReceivedOnNext()).hasSize(6);

		testSubscriber.request(5);
		assertThat(testSubscriber.getReceivedOnNext()).hasSize(10);
	}

	@Test
	void asyncFusedUpstreamGetsClearedOnCancellation() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.builder().requireFusion(Fuseable.ASYNC).build();

		@SuppressWarnings("unchecked")
		Fuseable.QueueSubscription<Integer> queueSubscription = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(queueSubscription.requestFusion(Mockito.anyInt())).thenReturn(Fuseable.ASYNC);

		testSubscriber.onSubscribe(queueSubscription);

		testSubscriber.cancel();

		Mockito.verify(queueSubscription).clear();
	}

	@Test
	void drainAsyncWithOnError() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.builder().requireFusion(Fuseable.ASYNC).build();

		@SuppressWarnings("unchecked")
		Fuseable.QueueSubscription<Integer> queueSubscription = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(queueSubscription.requestFusion(Mockito.anyInt())).thenReturn(Fuseable.ASYNC);
		Mockito.when(queueSubscription.poll()).thenReturn(1, 2, 3, 4, null);

		testSubscriber.onSubscribe(queueSubscription);

		testSubscriber.onNext(null); //trigger async fusion
		testSubscriber.onError(new IllegalStateException("expected"));

		Mockito.verify(queueSubscription).clear();
		Mockito.verify(queueSubscription, times(5)).poll();

		assertThat(testSubscriber.getReceivedOnNext())
				.as("receivedOnNext")
				.containsExactly(1, 2, 3, 4);

		assertThat(testSubscriber.getTerminalSignal())
				.as("terminal signal")
				.matches(Signal::isOnError, "isOnError")
				.satisfies(s -> assertThat(s.getThrowable()).isInstanceOf(IllegalStateException.class).hasMessage("expected"));
	}

	@Test
	@Tag("slow") //potentially slow due to timeout 10s
	void drainAsyncBadOnErrorDuringPollNotifiesAndClears() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.builder().requireFusion(Fuseable.ASYNC).build();

		@SuppressWarnings("unchecked")
		Fuseable.QueueSubscription<Integer> queueSubscription = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(queueSubscription.requestFusion(Mockito.anyInt())).thenReturn(Fuseable.ASYNC);

		final AtomicInteger count = new AtomicInteger();
		final CountDownLatch waitForPoll = new CountDownLatch(1);
		final CountDownLatch waitForOnError = new CountDownLatch(1);
		Mockito.when(queueSubscription.poll())
				.thenAnswer(args -> {
					int answer = count.incrementAndGet();
					if (answer == 3) {
						waitForPoll.countDown();
						waitForOnError.await(1, TimeUnit.SECONDS);
					}
					if (answer == 5) {
						return null;
					}
					return answer;
				});

		testSubscriber.onSubscribe(queueSubscription);

		//we only use RaceTestUtils for the convenience of blocking until both tasks have finished,
		// since we're doing our own artificial synchronization
		RaceTestUtils.race(
				() -> testSubscriber.onNext(null),
				() -> {
					try {
						if (waitForPoll.await(1, TimeUnit.SECONDS)) {
							testSubscriber.onError(new IllegalStateException("expected"));
							waitForOnError.countDown();
						}
					}
					catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
		);

		assertThatCode(() -> testSubscriber.block(Duration.ofSeconds(10)))
				.as("block(10s)")
				.doesNotThrowAnyException();

		Mockito.verify(queueSubscription).clear();
		Mockito.verify(queueSubscription, times(5)).poll();

		assertThat(testSubscriber.getReceivedOnNext())
				.as("receivedOnNext")
				.containsExactly(1, 2, 3, 4);

		assertThat(testSubscriber.getProtocolErrors())
				.as("protocol errors")
				.singleElement()
				.matches(Signal::isOnError, "isOnError")
				.satisfies(s -> assertThat(s.getThrowable()).isInstanceOf(IllegalStateException.class).hasMessage("expected"))
				.isSameAs(testSubscriber.getTerminalSignal()); //that last assertion ensures we registered the terminal signal
	}

	@Test
	@Tag("slow") //potentially slow due to timeout 10s
	void drainAsyncBadOnCompleteDuringPollNotifiesAndClears() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.builder().requireFusion(Fuseable.ASYNC).build();

		@SuppressWarnings("unchecked")
		Fuseable.QueueSubscription<Integer> queueSubscription = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(queueSubscription.requestFusion(Mockito.anyInt())).thenReturn(Fuseable.ASYNC);

		final AtomicInteger count = new AtomicInteger();
		final CountDownLatch waitForPoll = new CountDownLatch(1);
		final CountDownLatch waitForComplete = new CountDownLatch(1);
		Mockito.when(queueSubscription.poll())
				.thenAnswer(args -> {
					int answer = count.incrementAndGet();
					if (answer == 2) {
						waitForPoll.countDown();
						waitForComplete.await(1, TimeUnit.SECONDS);
					}
					if (answer == 5) {
						return null;
					}
					return answer;
				});

		testSubscriber.onSubscribe(queueSubscription);

		//we only use RaceTestUtils for the convenience of blocking until both tasks have finished,
		// since we're doing our own artificial synchronization
		RaceTestUtils.race(
				() -> testSubscriber.onNext(null),
				() -> {
					try {
						if (waitForPoll.await(1, TimeUnit.SECONDS)) {
							testSubscriber.onComplete();
							waitForComplete.countDown();
						}
					}
					catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
		);

		assertThatCode(() -> testSubscriber.block(Duration.ofSeconds(10)))
				.as("block(10s)")
				.doesNotThrowAnyException();

		Mockito.verify(queueSubscription).clear();
		Mockito.verify(queueSubscription, times(5)).poll();

		assertThat(testSubscriber.getReceivedOnNext())
				.as("receivedOnNext")
				.containsExactly(1, 2, 3, 4);

		assertThat(testSubscriber.getProtocolErrors())
				.as("protocol errors")
				.singleElement()
				.matches(Signal::isOnComplete, "isOnComplete")
				.isSameAs(testSubscriber.getTerminalSignal()); //that last assertion ensures we registered the terminal signal
	}

	@Test
	@Tag("slow") //potentially slow due to timeout 10s
	void drainAsyncBadTerminalNotifiesAndClearsOnceProducedRequestedAmount() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.builder()
				.initialRequest(3L)
				.requireFusion(Fuseable.ASYNC)
				.build();

		@SuppressWarnings("unchecked")
		Fuseable.QueueSubscription<Integer> queueSubscription = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(queueSubscription.requestFusion(Mockito.anyInt())).thenReturn(Fuseable.ASYNC);

		final AtomicInteger count = new AtomicInteger();
		final CountDownLatch waitForPoll = new CountDownLatch(1);
		final CountDownLatch waitForComplete = new CountDownLatch(1);
		Mockito.when(queueSubscription.poll())
				.thenAnswer(args -> {
					int answer = count.incrementAndGet();
					if (answer == 1) {
						waitForPoll.countDown();
						waitForComplete.await(1, TimeUnit.SECONDS);
					}
					if (answer == 10) {
						return null;
					}
					return answer;
				});

		testSubscriber.onSubscribe(queueSubscription);

		//we only use RaceTestUtils for the convenience of blocking until both tasks have finished,
		// since we're doing our own artificial synchronization
		RaceTestUtils.race(
				() -> testSubscriber.onNext(null),
				() -> {
					try {
						if (waitForPoll.await(1, TimeUnit.SECONDS)) {
							testSubscriber.onComplete();
							waitForComplete.countDown();
						}
					}
					catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
		);

		assertThatCode(() -> testSubscriber.block(Duration.ofSeconds(10)))
				.as("block(10s)")
				.doesNotThrowAnyException();

		Mockito.verify(queueSubscription).clear();
		Mockito.verify(queueSubscription, times(3)).poll();

		assertThat(testSubscriber.getReceivedOnNext())
				.as("receivedOnNext")
				.containsExactly(1, 2, 3);

		assertThat(testSubscriber)
				.matches(TestSubscriber::isTerminatedComplete, "isTerminatedComplete");

		//terminal onComplete is registered as a protocol error
		assertThat(testSubscriber.getProtocolErrors())
				.as("protocol errors")
				.singleElement()
				.isSameAs(testSubscriber.getTerminalSignal());
	}

	@Test
	@Tag("slow") //potentially slow due to timeout 10s
	void drainAsyncCancelledNotifiesAndClears() {
		TestSubscriber<Integer> testSubscriber = TestSubscriber.builder().requireFusion(Fuseable.ASYNC).build();

		@SuppressWarnings("unchecked")
		Fuseable.QueueSubscription<Integer> queueSubscription = Mockito.mock(Fuseable.QueueSubscription.class);
		Mockito.when(queueSubscription.requestFusion(Mockito.anyInt())).thenReturn(Fuseable.ASYNC);

		final AtomicInteger count = new AtomicInteger();
		final CountDownLatch waitForPoll = new CountDownLatch(1);
		final CountDownLatch waitForCancel = new CountDownLatch(1);
		Mockito.when(queueSubscription.poll())
				.thenAnswer(args -> {
					int answer = count.incrementAndGet();
					if (answer == 2) {
						waitForPoll.countDown();
						waitForCancel.await(1, TimeUnit.SECONDS);
					}
					if (answer == 5) {
						return null;
					}
					return answer;
				});

		testSubscriber.onSubscribe(queueSubscription);

		//we only use RaceTestUtils for the convenience of blocking until both tasks have finished,
		// since we're doing our own artificial synchronization
		RaceTestUtils.race(
				() -> testSubscriber.onNext(null),
				() -> {
					try {
						if (waitForPoll.await(1, TimeUnit.SECONDS)) {
							testSubscriber.cancel();
							waitForCancel.countDown();
						}
					}
					catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
		);

		assertThatCode(() -> testSubscriber.block(Duration.ofSeconds(10)))
				.as("block(10s)")
				.doesNotThrowAnyException();

		Mockito.verify(queueSubscription).clear();
		Mockito.verify(queueSubscription, times(2)).poll();

		assertThat(testSubscriber.getReceivedOnNext())
				.as("receivedOnNext")
				.containsExactly(1, 2);

		assertThat(testSubscriber)
				.matches(TestSubscriber::isCancelled, "isCancelled")
				.matches(ts -> !ts.isTerminated(), "not terminated");

		assertThat(testSubscriber.getProtocolErrors())
				.as("protocol errors")
				.isEmpty();
	}
}
