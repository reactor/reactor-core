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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * @author Simon Baslé
 */
class SinksTest {

	@Nested
	class MulticastNoWarmup {

		private Sinks.StandaloneFluxSink<Integer> sink;
		private Flux<Integer> flux;

		@BeforeEach
		void createInstance() {
			sink = Sinks.multicastNoWarmup();
			flux = sink.asFlux();
		}

		@Test
		void fluxViewReturnsSameInstance() {
			assertThat(flux).isSameAs(sink.asFlux());
		}

		@Test
		void isAcceptingMoreThatOneSubscriber() {
			assertThatCode(() -> {
				flux.subscribe();
				flux.subscribe();
			}).doesNotThrowAnyException();
		}

		@Test
		void honorsMultipleSubscribersBackpressure()
				throws InterruptedException, ExecutionException {
			ExecutorService es = Executors.newFixedThreadPool(2);
			CountDownLatch requestLatch = new CountDownLatch(2);
			final Future<?> f1 = es.submit(() -> {
				AssertSubscriber<Integer> test1 = AssertSubscriber.create(2);
				flux.subscribe(test1);
				test1.assertNoValues();
				requestLatch.countDown();

				test1.awaitAndAssertNextValues(1, 2);
				try {
					Awaitility.await().atMost(2, TimeUnit.SECONDS)
					          .with().pollDelay(1, TimeUnit.SECONDS)
					          .untilAsserted(() -> test1.assertValueCount(2));
				}
				finally {
					test1.cancel();
				}
			});
			final Future<?> f2 = es.submit(() -> {
				AssertSubscriber<Integer> test2 = AssertSubscriber.create(1);
				flux.subscribe(test2);
				requestLatch.countDown();

				test2.awaitAndAssertNextValues(1);
				try {
					Awaitility.await().atMost(2, TimeUnit.SECONDS)
					          .with().pollDelay(1, TimeUnit.SECONDS)
					          .untilAsserted(() -> test2.assertValueCount(1));
				}
				finally {
					test2.cancel();
				}
			});

			requestLatch.await(1, TimeUnit.SECONDS);
			sink.next(1)
			    .next(2)
			    .next(3)
			    .next(4)
			    .complete();

			f1.get();
			f2.get();
		}

		@Test
		void doesNotReplayToLateSubscribers() {
			AssertSubscriber<Integer> s1 = AssertSubscriber.create();
			AssertSubscriber<Integer> s2 = AssertSubscriber.create();

			flux.subscribe(s1);
			sink.next(1).next(2).next(3);
			s1.assertValues(1, 2, 3);

			flux.subscribe(s2);
			s2.assertNoValues().assertNotComplete();

			sink.complete();
			s1.assertValueCount(3).assertComplete();
			s2.assertNoValues().assertComplete();
		}

		@Test
		public void doesNotBufferBeforeFirstSubscriber() {
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			sink.next(1).next(2).next(3);
			flux.subscribe(first);

			first.assertNoValues().assertNotComplete();

			sink.complete();
			first.assertNoValues().assertComplete();
		}

		@Test
		public void immediatelyCompleteFirstSubscriber() {
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			sink.next(1).complete();
			flux.subscribe(first);

			first.assertNoValues().assertComplete();
		}

		@Test
		public void immediatelyErrorFirstSubscriber() {
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			sink.next(1).error(new IllegalStateException("boom"));
			flux.subscribe(first);

			first.assertNoValues().assertErrorMessage("boom");
		}

		@Test
		public void immediatelyCompleteLateSubscriber() {
			flux.subscribe(); //first subscriber
			AssertSubscriber<Integer> late = AssertSubscriber.create();

			sink.next(1).complete();
			flux.subscribe(late);

			late.assertNoValues().assertComplete();
		}

		@Test
		public void immediatelyErrorLateSubscriber() {
			flux.onErrorReturn(-1).subscribe(); //first subscriber, ignore errors
			AssertSubscriber<Integer> late = AssertSubscriber.create();

			sink.next(1).error(new IllegalStateException("boom"));
			flux.subscribe(late);

			late.assertNoValues().assertErrorMessage("boom");
		}
	}

	@Nested
	class Multicast {

		private Sinks.StandaloneFluxSink<Integer> sink;
		private Flux<Integer> flux;

		@BeforeEach
		void createInstance() {
			sink = Sinks.multicast();
			flux = sink.asFlux();
		}

		@Test
		void fluxViewReturnsSameInstance() {
			assertThat(flux).isSameAs(sink.asFlux());
		}

		@Test
		void isAcceptingMoreThatOneSubscriber() {
			assertThatCode(() -> {
				flux.subscribe();
				flux.subscribe();
			}).doesNotThrowAnyException();
		}

		@Test
		void honorsMultipleSubscribersBackpressure()
				throws InterruptedException, ExecutionException {
			ExecutorService es = Executors.newFixedThreadPool(2);
			CountDownLatch requestLatch = new CountDownLatch(2);
			final Future<?> f1 = es.submit(() -> {
				AssertSubscriber<Integer> test1 = AssertSubscriber.create(2);
				flux.subscribe(test1);
				test1.assertNoValues();
				requestLatch.countDown();

				test1.awaitAndAssertNextValues(1, 2);
				try {
					Awaitility.await().atMost(2, TimeUnit.SECONDS)
					          .with().pollDelay(1, TimeUnit.SECONDS)
					          .untilAsserted(() -> test1.assertValueCount(2));
				}
				finally {
					test1.cancel();
				}
			});
			final Future<?> f2 = es.submit(() -> {
				AssertSubscriber<Integer> test2 = AssertSubscriber.create(1);
				flux.subscribe(test2);
				requestLatch.countDown();

				test2.awaitAndAssertNextValues(1);
				try {
					Awaitility.await().atMost(2, TimeUnit.SECONDS)
					          .with().pollDelay(1, TimeUnit.SECONDS)
					          .untilAsserted(() -> test2.assertValueCount(1));
				}
				finally {
					test2.cancel();
				}
			});

			requestLatch.await(1, TimeUnit.SECONDS);
			sink.next(1)
			    .next(2)
			    .next(3)
			    .next(4)
			    .complete();

			f1.get();
			f2.get();
		}

		@Test
		void doesNotReplayToLateSubscribers() {
			AssertSubscriber<Integer> s1 = AssertSubscriber.create();
			AssertSubscriber<Integer> s2 = AssertSubscriber.create();

			flux.subscribe(s1);
			sink.next(1).next(2).next(3);
			s1.assertValues(1, 2, 3);

			flux.subscribe(s2);
			s2.assertNoValues().assertNotComplete();

			sink.complete();
			s1.assertValueCount(3).assertComplete();
			s2.assertNoValues().assertComplete();
		}

		@Test
		public void doesBufferBeforeFirstSubscriber() {
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			sink.next(1).next(2).next(3);
			flux.subscribe(first);

			first.assertValues(1, 2, 3).assertNotComplete();

			sink.complete();
			first.assertComplete();
		}

		@Test
		public void replayAndCompleteFirstSubscriber() {
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			sink.next(1).complete();
			flux.subscribe(first);

			first.assertValues(1).assertComplete();
		}

		@Test //TODO is that acceptable that EmitterProcessor doesn't replay in case of errors?
		public void noReplayBeforeFirstSubscriberIfEarlyError() {
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			sink.next(1).next(2).next(3).error(new IllegalStateException("boom"));
			flux.subscribe(first);

			first.assertNoValues().assertErrorMessage("boom");
		}

		@Test
		public void immediatelyCompleteLateSubscriber() {
			flux.subscribe(); //first subscriber
			AssertSubscriber<Integer> late = AssertSubscriber.create();

			sink.next(1).complete();
			flux.subscribe(late);

			late.assertNoValues().assertComplete();
		}

		@Test
		public void immediatelyErrorLateSubscriber() {
			flux.onErrorReturn(-1).subscribe(); //first subscriber, ignore errors
			AssertSubscriber<Integer> late = AssertSubscriber.create();

			sink.next(1).error(new IllegalStateException("boom"));
			flux.subscribe(late);

			late.assertNoValues().assertErrorMessage("boom");
		}
	}

	@Nested
	class MulticastReplayAll {

		private Sinks.StandaloneFluxSink<Integer> sink;
		private Flux<Integer> flux;

		@BeforeEach
		void createInstance() {
			sink = Sinks.multicastReplayAll();
			flux = sink.asFlux();
		}

		@Test
		void fluxViewReturnsSameInstance() {
			assertThat(flux).isSameAs(sink.asFlux());
		}

		@Test
		void isAcceptingMoreThatOneSubscriber() {
			assertThatCode(() -> {
				flux.subscribe();
				flux.subscribe();
			}).doesNotThrowAnyException();
		}

		@Test
		void honorsMultipleSubscribersBackpressure()
				throws InterruptedException, ExecutionException {
			ExecutorService es = Executors.newFixedThreadPool(2);
			CountDownLatch requestLatch = new CountDownLatch(2);
			final Future<?> f1 = es.submit(() -> {
				AssertSubscriber<Integer> test1 = AssertSubscriber.create(2);
				flux.subscribe(test1);
				test1.assertNoValues();
				requestLatch.countDown();

				test1.awaitAndAssertNextValues(1, 2);
				try {
					Awaitility.await().atMost(2, TimeUnit.SECONDS)
					          .with().pollDelay(1, TimeUnit.SECONDS)
					          .untilAsserted(() -> test1.assertValueCount(2));
				}
				finally {
					test1.cancel();
				}
			});
			final Future<?> f2 = es.submit(() -> {
				AssertSubscriber<Integer> test2 = AssertSubscriber.create(1);
				flux.subscribe(test2);
				requestLatch.countDown();

				test2.awaitAndAssertNextValues(1);
				try {
					Awaitility.await().atMost(2, TimeUnit.SECONDS)
					          .with().pollDelay(1, TimeUnit.SECONDS)
					          .untilAsserted(() -> test2.assertValueCount(1));
				}
				finally {
					test2.cancel();
				}
			});

			requestLatch.await(1, TimeUnit.SECONDS);
			sink.next(1)
			    .next(2)
			    .next(3)
			    .next(4)
			    .complete();

			f1.get();
			f2.get();
		}

		@Test
		void doesReplayAllToLateSubscribers() {
			AssertSubscriber<Integer> s1 = AssertSubscriber.create();
			AssertSubscriber<Integer> s2 = AssertSubscriber.create();

			flux.subscribe(s1);
			sink.next(1).next(2).next(3);
			s1.assertValues(1, 2, 3);

			flux.subscribe(s2);
			s2.assertValues(1, 2, 3).assertNotComplete();

			sink.complete();
			s1.assertValueCount(3).assertComplete();
			s2.assertValues(1, 2, 3).assertComplete();
		}

		@Test
		public void doesBufferBeforeFirstSubscriber() {
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			sink.next(1).next(2).next(3);
			flux.subscribe(first);

			first.assertValues(1, 2, 3).assertNotComplete();

			sink.complete();
			first.assertComplete();
		}

		@Test
		public void replayAndCompleteFirstSubscriber() {
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			sink.next(1).next(2).next(3).complete();
			flux.subscribe(first);

			first.assertValues(1, 2, 3).assertComplete();
		}

		@Test
		public void replayAndErrorFirstSubscriber() {
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			sink.next(1).next(2).next(3).error(new IllegalStateException("boom"));
			flux.subscribe(first);

			first.assertValues(1, 2, 3).assertErrorMessage("boom");
		}

		@Test
		public void replayAndCompleteLateSubscriber() {
			flux.subscribe(); //first subscriber
			AssertSubscriber<Integer> late = AssertSubscriber.create();

			sink.next(1).next(2).next(3).complete();
			flux.subscribe(late);

			late.assertValues(1, 2, 3).assertComplete();
		}

		@Test
		public void replayAndErrorLateSubscriber() {
			flux.onErrorReturn(-1).subscribe(); //first subscriber, ignore errors
			AssertSubscriber<Integer> late = AssertSubscriber.create();

			sink.next(1).next(2).next(3).error(new IllegalStateException("boom"));
			flux.subscribe(late);

			late.assertValues(1, 2, 3).assertErrorMessage("boom");
		}
	}

	@Nested
	class MulticastReplayTwo {

		private Sinks.StandaloneFluxSink<Integer> sink;
		private Flux<Integer> flux;

		@BeforeEach
		void createInstance() {
			sink = Sinks.multicastReplay(2);
			flux = sink.asFlux();
		}

		@Test
		void fluxViewReturnsSameInstance() {
			assertThat(flux).isSameAs(sink.asFlux());
		}

		@Test
		void isAcceptingMoreThatOneSubscriber() {
			assertThatCode(() -> {
				flux.subscribe();
				flux.subscribe();
			}).doesNotThrowAnyException();
		}

		@Test
		void honorsMultipleSubscribersBackpressure()
				throws InterruptedException, ExecutionException {
			ExecutorService es = Executors.newFixedThreadPool(2);
			CountDownLatch requestLatch = new CountDownLatch(2);
			final Future<?> f1 = es.submit(() -> {
				AssertSubscriber<Integer> test1 = AssertSubscriber.create(2);
				flux.subscribe(test1);
				test1.assertNoValues();
				requestLatch.countDown();

				test1.awaitAndAssertNextValues(1, 2);
				try {
					Awaitility.await().atMost(2, TimeUnit.SECONDS)
					          .with().pollDelay(1, TimeUnit.SECONDS)
					          .untilAsserted(() -> test1.assertValueCount(2));
				}
				finally {
					test1.cancel();
				}
			});
			final Future<?> f2 = es.submit(() -> {
				AssertSubscriber<Integer> test2 = AssertSubscriber.create(1);
				flux.subscribe(test2);
				requestLatch.countDown();

				test2.awaitAndAssertNextValues(1);
				try {
					Awaitility.await().atMost(2, TimeUnit.SECONDS)
					          .with().pollDelay(1, TimeUnit.SECONDS)
					          .untilAsserted(() -> test2.assertValueCount(1));
				}
				finally {
					test2.cancel();
				}
			});

			requestLatch.await(1, TimeUnit.SECONDS);
			sink.next(1)
			    .next(2)
			    .next(3)
			    .next(4)
			    .complete();

			f1.get();
			f2.get();
		}

		@Test
		void doesReplayLimitedHistoryToLateSubscribers() {
			AssertSubscriber<Integer> s1 = AssertSubscriber.create();
			AssertSubscriber<Integer> s2 = AssertSubscriber.create();

			flux.subscribe(s1);
			sink.next(1).next(2).next(3);
			s1.assertValues(1, 2, 3);

			flux.subscribe(s2);
			s2.assertValues(2, 3).assertNotComplete();

			sink.complete();
			s1.assertValueCount(3).assertComplete();
			s2.assertValues(2, 3).assertComplete();
		}

		@Test
		public void doesBufferLimitedHistoryBeforeFirstSubscriber() {
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			sink.next(1).next(2).next(3);
			flux.subscribe(first);

			first.assertValues(2, 3).assertNotComplete();

			sink.complete();
			first.assertComplete();
		}

		@Test
		public void replayLimitedHistoryAndCompleteFirstSubscriber() {
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			sink.next(1).next(2).next(3).complete();
			flux.subscribe(first);

			first.assertValues(2, 3).assertComplete();
		}

		@Test
		public void replayLimitedHistoryAndErrorFirstSubscriber() {
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			sink.next(1).next(2).next(3).error(new IllegalStateException("boom"));
			flux.subscribe(first);

			first.assertValues(2, 3).assertErrorMessage("boom");
		}

		@Test
		public void replayLimitedHistoryAndCompleteLateSubscriber() {
			flux.subscribe(); //first subscriber
			AssertSubscriber<Integer> late = AssertSubscriber.create();

			sink.next(1).next(2).next(3).complete();
			flux.subscribe(late);

			late.assertValues(2, 3).assertComplete();
		}

		@Test
		public void replayLimitedHistoryAndErrorLateSubscriber() {
			flux.onErrorReturn(-1).subscribe(); //first subscriber, ignore errors
			AssertSubscriber<Integer> late = AssertSubscriber.create();

			sink.next(1).next(2).next(3).error(new IllegalStateException("boom"));
			flux.subscribe(late);

			late.assertValues(2, 3).assertErrorMessage("boom");
		}
	}

	@Nested
	class Unicast {

		private Sinks.StandaloneFluxSink<Integer> sink;
		private Flux<Integer> flux;

		@BeforeEach
		void createInstance() {
			sink = Sinks.unicast();
			flux = sink.asFlux();
		}

		@Test
		void fluxViewReturnsSameInstance() {
			assertThat(flux).isSameAs(sink.asFlux());
		}

		@Test
		void isRejectingMoreThatOneSubscriber() {
			assertThatCode(() -> flux.subscribe()).doesNotThrowAnyException();
			StepVerifier.create(flux, 0)
			            .verifyErrorMessage("UnicastProcessor allows only a single Subscriber");
		}

		@Test
		void honorsSubscriberBackpressure()
				throws InterruptedException, ExecutionException {
			ExecutorService es = Executors.newFixedThreadPool(2);
			CountDownLatch requestLatch = new CountDownLatch(1);
			final Future<?> future = es.submit(() -> {
				AssertSubscriber<Integer> test = AssertSubscriber.create(2);
				flux.subscribe(test);
				test.assertNoValues();
				requestLatch.countDown();

				test.awaitAndAssertNextValues(1, 2);
				try {
					Awaitility.await().atMost(2, TimeUnit.SECONDS)
					          .with().pollDelay(1, TimeUnit.SECONDS)
					          .untilAsserted(() -> test.assertValueCount(2));
				}
				finally {
					test.cancel();
				}
			});

			requestLatch.await(1, TimeUnit.SECONDS);
			sink.next(1)
			    .next(2)
			    .next(3)
			    .next(4)
			    .complete();

			future.get();
		}

		@Test
		public void doesBufferAllBeforeFirstSubscriber() {
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			for (int i = 0; i < Queues.SMALL_BUFFER_SIZE + 5; i++) {
				sink.next(i);
			}
			flux.subscribe(first);

			first.assertValueCount(Queues.SMALL_BUFFER_SIZE + 5).assertNotComplete();

			sink.complete();
			first.assertComplete();
		}

		@Test
		public void replayAllAndCompleteFirstSubscriber() {
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			sink.next(1).next(2).next(3).complete();
			flux.subscribe(first);

			first.assertValues(1, 2, 3).assertComplete();
		}

		@Test
		public void replayAllAndErrorFirstSubscriber() {
			AssertSubscriber<Integer> first = AssertSubscriber.create();

			sink.next(1).next(2).next(3).error(new IllegalStateException("boom"));
			flux.subscribe(first);

			first.assertValues(1, 2, 3).assertErrorMessage("boom");
		}
	}

	//FIXME trigger mono sink tests

}