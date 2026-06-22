/*
 * Copyright (c) 2016-2026 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.LongStream;

import org.assertj.core.api.Assertions;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class FluxWindowTimeoutTest {

	@Test
	public void windowTimeoutWithBackPressureFromCore() throws InterruptedException {
		// -- The Event Producer
		//    The producer emitting requested events to downstream but with a delay of 250ms between each emission.
		//
		final int eventProduceDelayInMillis = 250;
		Flux<String> eventProducer = Flux.<String>create(sink -> {
			sink.onRequest(request -> {
				if (request != Long.MAX_VALUE) {
					System.out.println("Backpressure Request(" + request + ")");
					LongStream.range(0, request)
					          .mapToObj(String::valueOf)
					          .forEach(message -> {
						          try {
							          TimeUnit.MILLISECONDS.sleep(eventProduceDelayInMillis);
						          } catch (InterruptedException e) {
							          e.printStackTrace();
						          }
						          System.out.println("Producing:" + message);
						          sink.next(message);
					          });
				} else {
					sink.error(new RuntimeException("No_Backpressure unsupported"));
				}
			});
		}).subscribeOn(Schedulers.boundedElastic());

		// -- The Event Consumer
		//    The consumer using windowTimeout that batches maximum 10 events with a max wait time of 1 second.
		//    Given the Event producer produces at most 4 events per second (due to 250 ms delay between events),
		//    the consumer should receive 3-4 events.
		//
		final int eventConsumeDelayInMillis = 0;
		final Scheduler scheduler = Schedulers.newBoundedElastic(10, 10000, "queued-tasks");
		final AtomicBoolean hasError = new AtomicBoolean(false);
		final Semaphore isCompleted = new Semaphore(1);
		isCompleted.acquire();

		Disposable subscription = eventProducer.windowTimeout(10, Duration.ofSeconds(1), true)
		                                       .concatMap(Flux::collectList, 0)
		                                       .publishOn(scheduler)
		                                       .subscribe(eventBatch -> {
			                                       for (String event : eventBatch) {
				                                       System.out.println("Consuming: " + event);
				                                       try {
					                                       TimeUnit.MILLISECONDS.sleep(eventConsumeDelayInMillis);
				                                       } catch (InterruptedException e) {
					                                       System.err.println("Could not sleep for delay. Error: " + e);
				                                       }
			                                       }
			                                       System.out.println("Completed batch.");
		                                       }, error -> {
			                                       System.err.println("Error: " + error);
			                                       hasError.set(true);
			                                       isCompleted.release();
		                                       }, () -> {
			                                       System.out.println("Completed.");
			                                       isCompleted.release();
		                                       });

		System.out.println("Running test...");
		final Duration TIME_TO_PUBLISH_EVENTS = Duration.ofMinutes(1);
		try {
			assertFalse(isCompleted.tryAcquire(TIME_TO_PUBLISH_EVENTS.toMinutes(), TimeUnit.MINUTES),
					"Should have been false because it would not error.");

			assertFalse(hasError.get(), "Should not have received an error.");
		} finally {
			subscription.dispose();
		}

		System.out.println("Completed test.");
	}

	@Test
	public void bufferWithTimeoutWhenDownstreamDemandIsLow() {
		StepVerifier.withVirtualTime(() ->
				            Flux.range(1, 4)
				                .concatMap(e -> Mono.delay(Duration.ofMillis(200))
				                                    .thenReturn(e))
				                .log("pre")
				                .windowTimeout(5, Duration.ofMillis(100), true)
				                .log("windowTimeout")
				                .concatMap(flux -> flux.log("in").collectList(), 0)
				                .log("Post"),
				            1)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .assertNext(s -> assertThat(s).isEmpty())
		            .expectNoEvent(Duration.ofMillis(300))
		            .thenRequest(1)
		            .assertNext(s -> assertThat(s).containsExactly(1))
		            .expectNoEvent(Duration.ofMillis(1000))
		            .thenRequest(1)
		            .assertNext(s -> assertThat(s).containsExactly(2))
		            .expectNoEvent(Duration.ofMillis(1000))
		            .thenRequest(1)
		            .assertNext(s -> assertThat(s).containsExactly(3, 4))
		            .verifyComplete();
	}

	@Test
	public void windowWithTimeoutAccumulateOnSize() {
		StepVerifier.withVirtualTime(() -> Flux.range(1, 6)
		                                       .delayElements(Duration.ofMillis(300))
		                                       .windowTimeout(5,
				                                       Duration.ofMillis(2000),
				                                       true)
		                                       .concatMap(Flux::buffer))
		            .thenAwait(Duration.ofMillis(1500))
		            .assertNext(s -> assertThat(s).containsExactly(1, 2, 3, 4, 5))
		            .thenAwait(Duration.ofMillis(2000))
		            .assertNext(s -> assertThat(s).containsExactly(6))
		            .verifyComplete();
	}

	@Test
	public void windowWithTimeoutAccumulateOnTime() {
		StepVerifier.withVirtualTime(() -> Flux.range(1, 8)
		                                       .delayElements(Duration.ofNanos(300))
		                                       .windowTimeout(14,
				                                       Duration.ofNanos(2000),
				                                       true)
		                                       .concatMap(Flux::buffer))
		            .thenAwait(Duration.ofNanos(2000))
		            .assertNext(s -> assertThat(s).containsExactly(1, 2, 3, 4, 5, 6))
		            .thenAwait(Duration.ofNanos(2000))
		            .assertNext(s -> assertThat(s).containsExactly(7, 8))
		            .verifyComplete();
	}

	@Test
	public void longEmptyEmitsEmptyWindowsRegularly() {
		StepVerifier.withVirtualTime(() -> Mono.delay(Duration.ofMillis(350))
		                                       .ignoreElement()
		                                       .as(Flux::from)
		                                       .windowTimeout(1000,
				                                       Duration.ofMillis(100),
				                                       true)
		                                       .concatMap(Flux::collectList))
		            .thenAwait(Duration.ofMinutes(1))
		            .assertNext(l -> assertThat(l).isEmpty())
		            .assertNext(l -> assertThat(l).isEmpty())
		            .assertNext(l -> assertThat(l).isEmpty())
		            .assertNext(l -> assertThat(l).isEmpty())
		            .verifyComplete();
	}

	@Test
	public void longDelaysStartEndEmitEmptyWindows() {
		StepVerifier.withVirtualTime(() -> Mono.just("foo")
		                                       .delayElement(Duration.ofMillis(400 + 400 + 300))
		                                       .concatWith(Mono.delay(Duration.ofMillis(100 + 400 + 100))
		                                                       .then(Mono.empty()))
				                               .log()
		                                       .windowTimeout(1000, Duration.ofMillis(400), true)
				            .log("b")
		                                       .flatMap(flux -> flux.doOnError(t -> {
												   t.toString();
		                                       }).log("inner").collectList())
				            .log("c"))
		            .thenAwait(Duration.ofHours(1))
		            .assertNext(l -> assertThat(l).isEmpty())
		            .assertNext(l -> assertThat(l).isEmpty())
		            .assertNext(l -> assertThat(l).containsExactly("foo"))
		            .assertNext(l -> assertThat(l).isEmpty())
		            .assertNext(l -> assertThat(l).isEmpty()) //closing window
		            .verifyComplete();
	}

	@Test
	public void windowWithTimeoutStartsTimerOnSubscription() {
		StepVerifier.withVirtualTime(() -> Mono.delay(Duration.ofMillis(300))
		                                       .thenMany(Flux.range(1, 3))
		                                       .delayElements(Duration.ofMillis(150))
		                                       .concatWith(Flux.range(4, 10)
		                                                       .delaySubscription(Duration.ofMillis(
				                                                       500)))
		                                       .windowTimeout(10,
				                                       Duration.ofMillis(500),
				                                       true)
		                                       .flatMap(Flux::collectList))
		            .expectSubscription()
		            .thenAwait(Duration.ofSeconds(100))
		            .assertNext(l -> assertThat(l).containsExactly(1))
		            .assertNext(l -> assertThat(l).containsExactly(2, 3))
		            .assertNext(l -> assertThat(l).containsExactly(4,
				            5,
				            6,
				            7,
				            8,
				            9,
				            10,
				            11,
				            12,
				            13))
		            .assertNext(l -> assertThat(l).isEmpty())
		            .verifyComplete();
	}

	@Test
	public void noDelayMultipleOfSize() {
		StepVerifier.create(Flux.range(1, 10)
		                        .windowTimeout(5, Duration.ofSeconds(1), true)
		                        .concatMap(Flux::collectList))
		            .assertNext(l -> assertThat(l).containsExactly(1, 2, 3, 4, 5))
		            .assertNext(l -> assertThat(l).containsExactly(6, 7, 8, 9, 10))
		            .verifyComplete();
	}

	@Test
	public void noDelayGreaterThanSize() {
		StepVerifier.create(Flux.range(1, 12)
		                        .windowTimeout(5, Duration.ofHours(1), true)
		                        .concatMap(Flux::collectList))
		            .assertNext(l -> assertThat(l).containsExactly(1, 2, 3, 4, 5))
		            .assertNext(l -> assertThat(l).containsExactly(6, 7, 8, 9, 10))
		            .assertNext(l -> assertThat(l).containsExactly(11, 12))
		            .verifyComplete();
	}

	@Test
	public void rejectedOnSubscription() {
		Scheduler testScheduler = new Scheduler() {
			@Override
			public Disposable schedule(Runnable task) {
				throw Exceptions.failWithRejected();
			}

			@Override
			public Worker createWorker() {
				return new Worker() {
					@Override
					public Disposable schedule(Runnable task) {
						throw Exceptions.failWithRejected();
					}

					@Override
					public void dispose() {

					}
				};
			}
		};

		StepVerifier.create(Flux.range(1, 3)
		                        .hide()
		                        .windowTimeout(10,
				                        Duration.ofMillis(500),
				                        testScheduler,
				                        false))
		            .expectNextCount(1)
		            .verifyError(RejectedExecutionException.class);
	}

	@Test
	public void testIssue912() {
		StepVerifier.withVirtualTime(() -> Flux.concat(Flux.just("#")
		                                                   .delayElements(Duration.ofMillis(
				                                                   20)),
				                                       Flux.range(1, 10),
				                                       Flux.range(11, 5)
				                                           .delayElements(Duration.ofMillis(15)))
		                                       .windowTimeout(10,
				                                       Duration.ofMillis(1),
				                                       true)
		                                       .concatMap(w -> w)
		                                       .log())
		            .thenAwait(Duration.ofMillis(95))
		            .expectNextCount(16)
		            .verifyComplete();
	}

	@Test
	public void rejectedDuringLifecycle() {
		AtomicBoolean reject = new AtomicBoolean();
		Scheduler testScheduler = new Scheduler() {
			@Override
			public Disposable schedule(Runnable task) {
				throw Exceptions.failWithRejected();
			}

			@Override
			public Worker createWorker() {
				return new Worker() {

					Worker delegate = Schedulers.boundedElastic()
					                            .createWorker();

					@Override
					public Disposable schedule(Runnable task) {
						throw Exceptions.failWithRejected();
					}

					@Override
					public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
						if (reject.get()) {
							throw Exceptions.failWithRejected();
						}
						return delegate.schedule(task, delay, unit);
					}

					@Override
					public void dispose() {
						delegate.dispose();
					}
				};
			}
		};

		StepVerifier.create(Flux.range(1, 3)
		                        .hide()
		                        .windowTimeout(2,
				                        Duration.ofSeconds(2),
				                        testScheduler,
				                        true)
		                        .concatMap(w -> {
			                        reject.set(true);
			                        return w.collectList();
		                        }))
		            .verifyError(RejectedExecutionException.class);
	}

	@Test
	public void scanOperator() {
		FluxWindowTimeout<Integer> test = new FluxWindowTimeout<>(Flux.just(1),
				123,
				100,
				TimeUnit.MILLISECONDS,
				Schedulers.immediate(),
				true);

		assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.immediate());
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}

	private static final class MyWorker implements Scheduler.Worker, Scannable {

		@Override
		public void dispose() {
		}

		@Override
		public @Nullable Object scanUnsafe(Attr key) {
			return null;
		}

		@Override
		public Disposable schedule(Runnable task) {
			return null;
		}
	}

	private static final class MyScheduler implements Scheduler, Scannable {

		static final Worker WORKER = new MyWorker();

		@Override
		public Disposable schedule(Runnable task) {
			task.run();
			return Disposables.disposed();
		}

		@Override
		public Worker createWorker() {
			return WORKER;
		}

		@Override
		public @Nullable Object scanUnsafe(Attr key) {
			return null;
		}
	}

	@Test
	public void scanMainSubscriber() {
		Scheduler scheduler = new MyScheduler();
		CoreSubscriber<Flux<Integer>> actual = new LambdaSubscriber<>(null, e -> {
		}, null, null);
		FluxWindowTimeout.WindowTimeoutSubscriber<Integer> test =
				new FluxWindowTimeout.WindowTimeoutSubscriber<>(actual,
						123,
						Long.MAX_VALUE,
						TimeUnit.MILLISECONDS,
						scheduler);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		Assertions.assertThat(test.scan(Scannable.Attr.RUN_ON))
		          .isSameAs(scheduler.createWorker());
		Assertions.assertThat(test.scan(Scannable.Attr.PARENT))
		          .isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL))
		          .isSameAs(actual);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE))
		          .isSameAs(Scannable.Attr.RunStyle.ASYNC);

		Assertions.assertThat(test.scan(Scannable.Attr.CAPACITY))
		          .isEqualTo(123);
		test.requested = 35;
		Assertions.assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM))
		          .isEqualTo(35);
		Assertions.assertThat(test.scan(Scannable.Attr.BUFFERED))
		          .isEqualTo(0);
		test.onNext(1);
		test.onNext(2);
		Assertions.assertThat(test.inners()
		                          .findFirst()
		                          .get()
		                          .scan(Scannable.Attr.BUFFERED))
		          .isEqualTo(2);
		Assertions.assertThat(test.inners()
		                          .findFirst()
		                          .get()
		                          .scan(Scannable.Attr.CANCELLED))
		          .isEqualTo(false);
		Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED))
		          .isFalse();
		test.onComplete();
		Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED))
		          .isTrue();

		Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED))
		          .isFalse();
		test.cancel();
		Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED))
		          .isTrue();
	}

	@Test
	public void sourceError() {
		TestPublisher<Integer> source = TestPublisher.create();

		StepVerifier.create(source.flux()
		                          .windowTimeout(2, Duration.ofSeconds(1), true)
		                          .flatMap(Flux::materialize)
		            )
					.then(() -> source.next(1))
					.expectNext(Signal.next(1))
		            .then(() -> source.error(
				            new IllegalStateException("expected failure")))
		            // failure observed by window
		            .expectNextMatches(signalSourceErrorMessage("expected failure"))
		            // failure observed by main Flux
		            .expectErrorMessage("expected failure")
		            .verify();

		source.assertNoSubscribers();
	}

	private <T> Predicate<? super Signal<T>> signalSourceErrorMessage(String expectedMessage) {
		return signal -> signal.isOnError()
				&& signal.getThrowable() != null
				&& signal.getThrowable().getCause() != null
				&& expectedMessage.equals(signal.getThrowable().getCause().getMessage());
	}

	@Test
	void windowTimeoutFairBackpressure_unsentWindowDeliveredAfterIndexWrap() {
		// Test for the 20-bit index wrap-around hang.
		// Scenario: the unsent-window path in drain() is hit exactly when nextWindowIndex
		// wraps from 1_048_575 to 0.
		//
		// We use state injection (same package) to avoid actually cycling through
		// 1_048_576 windows in a test.

		VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
		int maxIndex =
				(int) FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber.NEXT_WINDOW_INDEX_MASK;

		List<Flux<Integer>> deliveredWindows = new ArrayList<>();

		// Outer subscriber that captures windows without auto-requesting.
		CoreSubscriber<Flux<Integer>> outer = new CoreSubscriber<Flux<Integer>>() {
			@Override public void onSubscribe(Subscription s) { /* no auto-request */ }
			@Override public void onNext(Flux<Integer> w)      { deliveredWindows.add(w); }
			@Override public void onError(Throwable t)         { throw Exceptions.propagate(t); }
			@Override public void onComplete()                 {}
		};

		FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<Integer> sub =
				new FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber<>(
						outer, 1000, 1, TimeUnit.SECONDS, scheduler, null);

		// Provide a no-op upstream subscription so s.request() calls inside drain don't NPE.
		sub.onSubscribe(Operators.emptySubscription());

		// Inject state: both indices at maxIndex, unsent window flag set, no demand.
		// This simulates the subscriber having processed 1_048_575 windows already.
		long preWrapState =
				FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber.NEXT_WINDOW_INDEX_MASK
						| FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber.ACTIVE_WINDOW_INDEX_MASK
						| FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber.HAS_UNSENT_WINDOW;

		FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber.STATE.set(sub, preWrapState);

		// Install an InnerWindow whose index matches nextWindowIndex in the injected state.
		FluxWindowTimeout.InnerWindow<Integer> preWrapWindow =
				new FluxWindowTimeout.InnerWindow<>(1000, sub, maxIndex, true, null);
		sub.window = preWrapWindow;

		// Fire the timeout for the pre-wrap window (no downstream demand at this point).
		// tryCreateNextWindow wraps nextWindowIndex from 1_048_575 to 0.
		// drain() sees n == 0 and creates an UNSENT window at index 0.
		preWrapWindow.run();

		assertThat(FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber.nextWindowIndex(sub.state))
				.as("nextWindowIndex wrapped to 0").isZero();
		// activeWindowIndex stays at MAX.
		assertThat(FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber.activeWindowIndex(sub.state))
				.as("activeWindowIndex stays at MAX — commitWork did not run").isEqualTo(maxIndex);
		assertThat(FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber.hasUnsentWindow(sub.state))
				.as("original unsent window at MAX is still pending delivery").isTrue();
		assertThat(deliveredWindows).as("no demand yet — nothing delivered").isEmpty();

		// Demand arrives: the unsent window at MAX is delivered.
		sub.request(1);

		assertThat(deliveredWindows)
				.as("unsent window at MAX delivered").hasSize(1);
		assertThat(sub.window.index)
				.as("current window must be the wrapped index 0, not the stale MAX window").isZero();
		// window 0 was created as unsent (shouldBeUnsent = true since n == 0 after delivering
		// index = max).
		assertThat(FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber.hasUnsentWindow(sub.state))
				.as("window 0 is now the pending unsent window").isTrue();

		// Advance time: window 0 times out and window 1 created as unsent (still no demand).
		scheduler.advanceTimeBy(Duration.ofSeconds(1));

		assertThat(FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber.nextWindowIndex(sub.state))
				.as("nextWindowIndex is now 1, stream progresses normally past the wrap").isEqualTo(1);
		assertThat(FluxWindowTimeout.WindowTimeoutWithBackpressureSubscriber.hasUnsentWindow(sub.state))
				.as("window 1 is unsent, awaiting demand").isTrue();

		// Deliver window 1 — proves the stream is not hung.
		sub.request(1);

		assertThat(deliveredWindows)
				.as("window 1 delivered: stream did not hang after index wrap").hasSize(2);
		assertThat(sub.window.index)
				.as("active window is now index 1").isEqualTo(1);
	}
}
