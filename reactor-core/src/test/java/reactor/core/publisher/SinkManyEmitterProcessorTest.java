/*
 * Copyright (c) 2016-2025 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.AutoDisposingExtension;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.subscriber.TestSubscriber;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.*;
import static reactor.core.Scannable.Attr;
import static reactor.core.Scannable.Attr.*;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;
import static reactor.core.scheduler.Schedulers.DEFAULT_POOL_SIZE;

/**
 * @author Stephane Maldini
 * @author Simon Baslé
 */
class SinkManyEmitterProcessorTest {

	@RegisterExtension
	AutoDisposingExtension afterTest = new AutoDisposingExtension();

	@Test
	void smokeTestManyWithUpstream() {
		final Sinks.ManyWithUpstream<Integer> adapter = Sinks.unsafe().manyWithUpstream().multicastOnBackpressureBuffer();
		final TestSubscriber<Integer> testSubscriber1 = TestSubscriber.create();
		final TestSubscriber<Integer> testSubscriber2 = TestSubscriber.create();
		final Flux<Integer> upstream = Flux.range(1, 10);

		adapter.asFlux()
			.publishOn(Schedulers.parallel())
			.skip(8)
			.log()
			.subscribe(testSubscriber1);

		adapter.asFlux()
			.publishOn(Schedulers.parallel())
			.skipLast(8)
			.subscribe(testSubscriber2);

		adapter.subscribeTo(upstream);

		testSubscriber1.block();
		testSubscriber2.block();

		assertThat(adapter).isInstanceOf(SinkManyEmitterProcessor.class);
		assertThat(testSubscriber1.getReceivedOnNext()).as("ts1 onNexts").containsExactly(9, 10);
		assertThat(testSubscriber1.isTerminatedComplete()).as("ts1 isTerminatedComplete").isTrue();

		assertThat(testSubscriber2.getReceivedOnNext()).as("ts2 onNexts").containsExactly(1, 2);
		assertThat(testSubscriber2.isTerminatedComplete()).as("ts2 isTerminatedComplete").isTrue();
	}

	@Test
	void smokeTestSubscribeAndDispose() {
		final Sinks.ManyWithUpstream<Integer> adapter = Sinks.unsafe().manyWithUpstream().multicastOnBackpressureBuffer();
		final TestSubscriber<Integer> testSubscriber1 = TestSubscriber.create();
		final TestSubscriber<Integer> testSubscriber2 = TestSubscriber.create();
		final Flux<Integer> upstream = Flux.never();

		adapter.asFlux().subscribe(testSubscriber1);

		Disposable disposable = adapter.subscribeTo(upstream);

		assertThat(testSubscriber1.isTerminatedOrCancelled()).as("ts1 live before dispose");

		disposable.dispose();

		assertThat(disposable.isDisposed()).as("disposable isDisposed").isTrue();

		assertThat(testSubscriber1.expectTerminalError()).as("ts1 onError")
			.isInstanceOf(CancellationException.class)
			.hasMessage("the ManyWithUpstream sink had a Subscription to an upstream which has been manually cancelled");

		adapter.asFlux().subscribe(testSubscriber2);

		assertThat(testSubscriber1.expectTerminalError()).as("ts2 late subscription onError")
			.isInstanceOf(CancellationException.class)
			.hasMessage("the ManyWithUpstream sink had a Subscription to an upstream which has been manually cancelled");
	}

	@Test
	void subscribeToUpstreamTwiceSkipsSecondSubscription() {
		final Sinks.ManyWithUpstream<Integer> adapter = Sinks.unsafe().manyWithUpstream().multicastOnBackpressureBuffer(123, true);
		final TestPublisher<Integer> upstream1 = TestPublisher.create();
		final TestPublisher<Integer> upstream2 = TestPublisher.create();

		Disposable sub1 = adapter.subscribeTo(upstream1);

		assertThatIllegalStateException().isThrownBy(() -> adapter.subscribeTo(upstream2));

		assertThat(sub1.isDisposed()).as("first subscription active").isFalse();

		upstream1.assertMinRequested(123);

		upstream2.assertWasNotSubscribed();
		upstream2.assertWasNotRequested();
	}

	@Test
	void currentSubscriberCount() {
		Sinks.Many<Integer> sink = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);

		assertThat(sink.currentSubscriberCount()).isZero();

		sink.asFlux().subscribe();

		assertThat(sink.currentSubscriberCount()).isOne();

		sink.asFlux().subscribe();

		assertThat(sink.currentSubscriberCount()).isEqualTo(2);
	}

	// see https://github.com/reactor/reactor-core/issues/3028
	@Test
	void concurrentSubscriberDisposalDoesntLeak() {
		for (int repetition = 0; repetition < 20; repetition++) {
			Scheduler disposeScheduler = afterTest.autoDispose(Schedulers.newParallel("concurrentSubscriberDisposalDoesntLeak", 5));

			List<Disposable> toDisposeInMultipleThreads = new ArrayList<>();
			Sinks.Many<Integer> sink = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);

			for (int i = 0; i < 10; i++) {
				toDisposeInMultipleThreads.add(
					sink.asFlux().subscribe(null, null, null, Context.of("inner", "#" + i))
				);
			}

			toDisposeInMultipleThreads.forEach(disposable -> disposeScheduler.schedule(disposable::dispose));

			Awaitility.await().atMost(Duration.ofSeconds(2))
				.alias("no more subscribers")
				.untilAsserted(() -> assertThat(sink.currentSubscriberCount()).as("subscriberCount").isZero());
		}
	}

	//see https://github.com/reactor/reactor-core/issues/1364
	@Test
	void subscribeWithSyncFusionUpstreamFirst() {
		SinkManyEmitterProcessor<String> processor = new SinkManyEmitterProcessor<>(true, 16);

		StepVerifier.create(
				Mono.just("DATA")
				    .subscribeWith(processor)
				    .map(String::toLowerCase)
		)
		            .expectNext("data")
		            .expectComplete()
		            .verify(Duration.ofSeconds(1));

		assertThat(processor.blockFirst()).as("later subscription").isNull();
	}

	//see https://github.com/reactor/reactor-core/issues/1290
	@Test
	void subscribeWithSyncFusionSingle() {
		SinkManyEmitterProcessor<Integer> processor = new SinkManyEmitterProcessor<>(true, 16);

		StepVerifier.create(processor)
		            .then(() -> Flux.just(1).subscribe(processor))
		            .expectNext(1)
		            .expectComplete()
		            .verify(Duration.ofSeconds(1));
	}

	//see https://github.com/reactor/reactor-core/issues/1290
	@Test
	void subscribeWithSyncFusionMultiple() {
		SinkManyEmitterProcessor<Integer> processor = new SinkManyEmitterProcessor<>(true, 16);

		StepVerifier.create(processor)
		            .then(() -> Flux.range(1, 5).subscribe(processor))
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify(Duration.ofSeconds(1));
	}

	//see https://github.com/reactor/reactor-core/issues/1290
	@Test
	void subscribeWithAsyncFusion() {
		SinkManyEmitterProcessor<Integer> processor = new SinkManyEmitterProcessor<>(true, 16);

		StepVerifier.create(processor)
		            .then(() -> Flux.range(1, 5).publishOn(Schedulers.boundedElastic()).subscribe(processor))
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify(Duration.ofSeconds(1));
	}

	@SuppressWarnings({"NullAway", "DataFlowIssue"}) // passing null on purpose
	@Test
	void emitNextNullWithAsyncFusion() {
		SinkManyEmitterProcessor<Integer> processor = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);

		// Any `QueueSubscription` capable of doing ASYNC fusion
		Fuseable.QueueSubscription<Object> queueSubscription = SinkManyUnicast.create();
		processor.onSubscribe(queueSubscription);

		// Expect the ASYNC fusion mode
		assertThat(processor.sourceMode).as("sourceMode").isEqualTo(Fuseable.ASYNC);

		processor.onNext(null);
		assertThatThrownBy(() -> processor.emitNext(null, FAIL_FAST)).isInstanceOf(NullPointerException.class);
	}

	@Test
	void testColdIdentityProcessor() throws InterruptedException {
		final int elements = 10;
		CountDownLatch latch = new CountDownLatch(elements + 1);

		SinkManyEmitterProcessor<Integer> processor = new SinkManyEmitterProcessor<>(true, 16);

		List<Integer> list = new ArrayList<>();

		processor.subscribe(new CoreSubscriber<Integer>() {
			Subscription s;

			@Override
			public void onSubscribe(Subscription s) {
				this.s = s;
				s.request(1);
			}

			@Override
			public void onNext(Integer integer) {
				synchronized (list) {
					list.add(integer);
				}
				latch.countDown();
				if (latch.getCount() > 0) {
					s.request(1);
				}
			}

			@Override
			public void onError(Throwable t) {
				t.printStackTrace();
			}

			@Override
			public void onComplete() {
				System.out.println("completed!");
				latch.countDown();
			}
		});

		Flux.range(1, 10)
		    .subscribe(processor);


		//stream.broadcastComplete();

		latch.await(8, TimeUnit.SECONDS);

		long count = latch.getCount();
		assertThat(latch.getCount())
				.as("Count > 0 : %d (%s), Running on %d CPUs", count, list, DEFAULT_POOL_SIZE)
				.isEqualTo(0L);

	}

	@SuppressWarnings({"NullAway", "DataFlowIssue"}) // passing null on purpose
	@Test
	void onNextNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE).onNext(null);
		});
	}

	@Test
	void onErrorNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE).onError(null);
		});
	}

	@Test
	void onSubscribeNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE).onSubscribe(null);
		});
	}

	@Test
	void subscribeNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE).subscribe((Subscriber<Object>) null);
		});
	}

	@Test
	void normal() {
		SinkManyEmitterProcessor<Integer> tp = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);
		StepVerifier.create(tp)
		            .then(() -> {
			            assertThat(tp.currentSubscriberCount()).as("has subscriber").isPositive();
			            assertThat(tp.isTerminated()).as("isTerminated").isFalse();
			            assertThat(tp.getError()).as("getError").isNull();
		            })
		            .then(() -> {
			            tp.onNext(1);
			            tp.onNext(2);
		            })
		            .expectNext(1, 2)
		            .then(() -> {
			            tp.onNext(3);
			            tp.onComplete();
		            })
		            .expectNext(3)
		            .expectComplete()
		            .verify();

		assertThat(tp.currentSubscriberCount()).as("has subscriber").isZero();
		assertThat(tp.isTerminated()).as("isTerminated").isTrue();
		assertThat(tp.getError()).as("getError").isNull();
	}

	@Test
	void normalBackpressured() {
		SinkManyEmitterProcessor<Integer> tp = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);
		StepVerifier.create(tp, 0L)
		            .then(() -> {
			            assertThat(tp.currentSubscriberCount()).as("has subscriber").isPositive();
			            assertThat(tp.isTerminated()).as("isTerminated").isFalse();
			            assertThat(tp.getError()).as("getError").isNull();
		            })
		            .then(() -> {
			            tp.onNext(1);
			            tp.onNext(2);
			            tp.onComplete();
		            })
		            .thenRequest(10L)
		            .expectNext(1, 2)
		            .expectComplete()
		            .verify();

		assertThat(tp.currentSubscriberCount()).as("has subscriber").isZero();
		assertThat(tp.isTerminated()).as("isTerminated").isTrue();
		assertThat(tp.getError()).as("getError").isNull();
	}

	@Test
	void normalAtomicRingBufferBackpressured() {
		SinkManyEmitterProcessor<Integer> tp = new SinkManyEmitterProcessor<>(true,100);
		StepVerifier.create(tp, 0L)
		            .then(() -> {
			            assertThat(tp.currentSubscriberCount()).as("has subscriber").isPositive();
			            assertThat(tp.isTerminated()).as("isTerminated").isFalse();
			            assertThat(tp.getError()).as("getError").isNull();
		            })
		            .then(() -> {
			            tp.onNext(1);
			            tp.onNext(2);
			            tp.onComplete();
		            })
		            .thenRequest(10L)
		            .expectNext(1, 2)
		            .expectComplete()
		            .verify();

		assertThat(tp.currentSubscriberCount()).as("has subscriber").isZero();
		assertThat(tp.isTerminated()).as("isTerminated").isTrue();
		assertThat(tp.getError()).as("getError").isNull();
	}


	@Test
	void failZeroBufferSize() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new SinkManyEmitterProcessor<>(true, 0))
			.withMessage("bufferSize must be strictly positive, was: 0");
	}

	@Test
	void failNegativeBufferSize() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new SinkManyEmitterProcessor<>(true, -1))
			.withMessage("bufferSize must be strictly positive, was: -1");
	}

	@Test
	void failNullTryEmitNext() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE).tryEmitNext(null);
		})
			.withMessage("tryEmitNext must be invoked with a non-null value");
	}

	@Test
	void failNullTryEmitError() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE).tryEmitError(null);
		})
			.withMessage("tryEmitError must be invoked with a non-null Throwable");
	}

	@Test
	void failDoubleError() {
		SinkManyEmitterProcessor<Integer> ep = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);
		StepVerifier.create(ep)
	                .then(() -> {
		                assertThat(ep.getError()).isNull();
						ep.onError(new Exception("test"));
						assertThat(ep.getError()).hasMessage("test");
						ep.onError(new Exception("test2"));
	                })
	                .expectErrorMessage("test")
	                .verifyThenAssertThat()
	                .hasDroppedErrorWithMessage("test2");
	}

	@Test
	void failCompleteThenError() {
		SinkManyEmitterProcessor<Integer> ep = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);
		StepVerifier.create(ep)
	                .then(() -> {
						ep.onComplete();
						ep.onComplete();//noop
						ep.onError(new Exception("test"));
	                })
	                .expectComplete()
	                .verifyThenAssertThat()
	                .hasDroppedErrorWithMessage("test");
	}

	static final List<String> DATA     = new ArrayList<>();
	static final int          MAX_SIZE = 100;

	static {
		for (int i = 1; i <= MAX_SIZE; i++) {
			DATA.add("" + i);
		}
	}

	@Test
	void testRed() {
		SinkManyEmitterProcessor<String> processor = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);
		AssertSubscriber<String> subscriber = AssertSubscriber.create(1);
		processor.subscribe(subscriber);

		Flux.fromIterable(DATA)
		    .log()
		    .subscribe(processor);

		subscriber.awaitAndAssertNextValues("1");
	}

	@Test
	void testGreen() {
		SinkManyEmitterProcessor<String> processor = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);
		AssertSubscriber<String> subscriber = AssertSubscriber.create(1);
		processor.subscribe(subscriber);

		Flux.fromIterable(DATA)
		    .log()
		    .subscribe(processor);


		subscriber.awaitAndAssertNextValues("1");
	}

	@Test
	void testHanging() {
		SinkManyEmitterProcessor<String> processor = new SinkManyEmitterProcessor<>(true, 2);

		AssertSubscriber<String> first = AssertSubscriber.create(0);
		processor.log("after-1").subscribe(first);

		AssertSubscriber<String> second = AssertSubscriber.create(0);
		processor.log("after-2").subscribe(second);

		Flux.fromIterable(DATA)
		    .log()
		    .subscribe(processor);

		second.request(1);
		second.assertNoValues();

		first.request(3);

		second.awaitAndAssertNextValues("1");

		second.cancel();
		first.awaitAndAssertNextValues("1", "2", "3");
		first.cancel();

		assertThat(processor.scanOrDefault(CANCELLED, false)).isTrue();
	}

	@Test
	void testNPE() {
		SinkManyEmitterProcessor<String> processor = new SinkManyEmitterProcessor<>(true, 8);
		AssertSubscriber<String> first = AssertSubscriber.create(1);
		processor.log().take(1, false).subscribe(first);

		AssertSubscriber<String> second = AssertSubscriber.create(3);
		processor.log().subscribe(second);

		Flux.fromIterable(DATA)
		    .log()
		    .subscribe(processor);


		first.awaitAndAssertNextValues("1");


		second.awaitAndAssertNextValues("1", "2", "3");
	}

	static class MyThread extends Thread {

		private final Flux<String> processor;

		private final CyclicBarrier barrier;

		private final int n;

		private volatile Throwable lastException;

		class MyUncaughtExceptionHandler implements UncaughtExceptionHandler {

			@Override
			public void uncaughtException(Thread t, Throwable e) {
				lastException = e;
			}

		}

		public MyThread(SinkManyEmitterProcessor<String> processor, CyclicBarrier barrier, int n, int index) {
			this.processor = processor.log("consuming."+index);
			this.barrier = barrier;
			this.n = n;
			setUncaughtExceptionHandler(new MyUncaughtExceptionHandler());
		}

		@Override
		public void run() {
			try {
				doRun();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		public void doRun() throws Exception {
			AssertSubscriber<String> subscriber = AssertSubscriber.create(5);
			processor.subscribe(subscriber);
			barrier.await();

			subscriber.request(3);
			subscriber.request(4);
			subscriber.request(1);

			subscriber
					.await()
					.assertValueCount(n)
					.assertComplete();
		}

		public @Nullable Throwable getLastException() {
			return lastException;
		}

	}

	@Test
	@Disabled
	void testRacing() throws Exception {
		int N_THREADS = 3;
		int N_ITEMS = 8;

		SinkManyEmitterProcessor<String> processor = new SinkManyEmitterProcessor<>(true, 4);
		List<String> data = new ArrayList<>();
		for (int i = 1; i <= N_ITEMS; i++) {
			data.add(String.valueOf(i));
		}

		Flux.fromIterable(data)
		    .log("publishing")
		    .subscribe(processor);

		CyclicBarrier barrier = new CyclicBarrier(N_THREADS);

		MyThread threads[] = new MyThread[N_THREADS];
		for (int i = 0; i < N_THREADS; i++) {
			threads[i] = new MyThread(processor, barrier, N_ITEMS, i);
			threads[i].start();
		}

		for (int j = 0; j < N_THREADS; j++) {
			threads[j].join();
			Throwable lastException = threads[j].getLastException();
			if (lastException != null) {
				lastException.printStackTrace();
				fail("Should not have encounterd exception");
			}
		}

	}

	@Test
	void testThreadAffinity() throws InterruptedException {
		int count = 10;
		Scheduler[] schedulers = new Scheduler[4];
		CountDownLatch[] latches = new CountDownLatch[schedulers.length];
		for (int i = 0; i < schedulers.length; i++) {
			schedulers[i] = Schedulers.newSingle("scheduler" + i + '-');
			int expectedCount = i == 1 ? count * 2 : count;
			latches[i] = new CountDownLatch(expectedCount);
		}
		SinkManyEmitterProcessor<Integer> processor = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);
		processor.publishOn(schedulers[0])
			 .share();
		processor.publishOn(schedulers[1])
				 .subscribe(i -> {
						assertThat(Thread.currentThread().getName().contains("scheduler1")).isTrue();
						latches[1].countDown();
					});
		for (int i = 0; i < count; i++)
			processor.onNext(i);
		processor.publishOn(schedulers[2])
				 .map(i -> {
						assertThat(Thread.currentThread().getName().contains("scheduler2")).isTrue();
						latches[2].countDown();
						return i;
					})
				 .publishOn(schedulers[3])
				 .doOnNext(i -> {
						assertThat(Thread.currentThread().getName().contains("scheduler3")).isTrue();
						latches[3].countDown();
					})
				 .subscribe();
		for (int i = 0; i < count; i++)
			processor.onNext(i);
		processor.onComplete();

		for (int i = 1; i < latches.length; i++)
			assertThat(latches[i].await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(latches[0].getCount()).isEqualTo(count);
	}

	@Test
	void createDefault() {
		SinkManyEmitterProcessor<Integer> processor = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);
		assertProcessor(processor, null, null);
	}

	@Test
	void createOverrideBufferSize() {
		int bufferSize = 1024;
		SinkManyEmitterProcessor<Integer> processor = new SinkManyEmitterProcessor<>(true, bufferSize);
		assertProcessor(processor, bufferSize, null);
	}

	@Test
	void createOverrideAutoCancel() {
		boolean autoCancel = false;
		SinkManyEmitterProcessor<Integer> processor = new SinkManyEmitterProcessor<>(autoCancel, Queues.SMALL_BUFFER_SIZE);
		assertProcessor(processor, null, autoCancel);
	}

	@Test
	void createOverrideAll() {
		int bufferSize = 1024;
		boolean autoCancel = false;
		SinkManyEmitterProcessor<Integer> processor = new SinkManyEmitterProcessor<>(autoCancel, bufferSize);
		assertProcessor(processor, bufferSize, autoCancel);
	}

	void assertProcessor(SinkManyEmitterProcessor<Integer> processor,
						 @Nullable Integer bufferSize,
						 @Nullable Boolean autoCancel) {
		int expectedBufferSize = bufferSize != null ? bufferSize : Queues.SMALL_BUFFER_SIZE;
		boolean expectedAutoCancel = autoCancel != null ? autoCancel : true;

		assertThat(processor.prefetch).isEqualTo(expectedBufferSize);
		assertThat(processor.autoCancel).isEqualTo(expectedAutoCancel);
	}

	@Test
	void scanMain() {
		SinkManyEmitterProcessor<Integer> test = new SinkManyEmitterProcessor<>(true, 123);
		assertThat(test.scan(BUFFERED)).isEqualTo(0);
		assertThat(test.scan(CANCELLED)).isFalse();
		assertThat(test.scan(PREFETCH)).isEqualTo(123);
		assertThat(test.scan(CAPACITY)).isEqualTo(123);

		Disposable d1 = test.subscribe();

		test.tryEmitNext(2).orThrow();
		test.tryEmitNext(3).orThrow();
		test.tryEmitNext(4).orThrow();

		assertThat(test.scan(BUFFERED)).isEqualTo(0);

		AtomicReference<Subscription> d2 = new AtomicReference<>();
		test.subscribe(new CoreSubscriber<Integer>() {
			@Override
			public void onSubscribe(Subscription s) {
				d2.set(s);
			}

			@Override
			public void onNext(Integer integer) {

			}

			@Override
			public void onError(Throwable t) {

			}

			@Override
			public void onComplete() {

			}
		});
		test.tryEmitNext(5).orThrow();
		test.tryEmitNext(6).orThrow();
		test.tryEmitNext(7).orThrow();

		assertThat(test.scan(BUFFERED)).isEqualTo(3);
		assertThat(test.scan(TERMINATED)).isFalse();

		test.tryEmitComplete().orThrow();

		assertThat(test.scan(TERMINATED)).isFalse();

		d1.dispose();
		d2.get().cancel();
		assertThat(test.scan(TERMINATED)).isTrue();

		//other values
		assertThat(test.scan(Scannable.Attr.PARENT)).isNotNull();
		assertThat(test.scan(Attr.ERROR)).isNull();
	}

	@Test
	void scanMainCancelled() {
		SinkManyEmitterProcessor<?> test = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);
		test.subscribe().dispose();

		assertThat(test.scan(CANCELLED)).isTrue();
		assertThat(test.isCancelled()).isTrue();
	}

	@Test
	void scanMainError() {
		SinkManyEmitterProcessor<?> test = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);
		test.tryEmitError(new IllegalStateException("boom")).orThrow();

		assertThat(test.scan(TERMINATED)).as("terminated").isTrue();
		assertThat(test.scan(Attr.ERROR)).hasMessage("boom");
	}

	@Test
	void inners() {
		Sinks.Many<Integer> sink = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);
		CoreSubscriber<Integer> notScannable = new BaseSubscriber<Integer>() {};
		InnerConsumer<Integer> scannable = new LambdaSubscriber<>(null, null, null, null);

		assertThat(sink.inners()).as("before subscriptions").isEmpty();

		sink.asFlux().subscribe(notScannable);
		sink.asFlux().subscribe(scannable);

		assertThat(sink.inners())
				.asInstanceOf(InstanceOfAssertFactories.LIST)
				.as("after subscriptions")
				.hasSize(2)
				.extracting(l -> (Object) ((SinkManyEmitterProcessor.EmitterInner<?>) l).actual)
				.containsExactly(notScannable, scannable);
	}

	//see https://github.com/reactor/reactor-core/issues/1528
	@Test
	void syncFusionFromInfiniteStream() {
		final Flux<Integer> flux = Flux.fromStream(Stream.iterate(0, i -> i + 1));
		final SinkManyEmitterProcessor<Integer> processor = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);

		StepVerifier.create(processor)
		            .then(() -> flux.subscribe(processor))
		            .thenConsumeWhile(i -> i < 10)
		            .expectNextCount(10)
		            .thenCancel()
		            .verify(Duration.ofSeconds(4));
	}

	//see https://github.com/reactor/reactor-core/issues/1528
	@Test
	void syncFusionFromInfiniteStreamAndTake() {
		final Flux<Integer> flux = Flux.fromStream(Stream.iterate(0, i -> i + 1))
				.take(10, false);
		final SinkManyEmitterProcessor<Integer> processor = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);
		flux.subscribe(processor);

		StepVerifier.create(processor)
		            .expectNextCount(10)
		            .expectComplete()
		            .verify(Duration.ofSeconds(4));
	}

	@Test
	void removeUnknownInnerIgnored() {
		SinkManyEmitterProcessor<Integer> processor = new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);
		SinkManyEmitterProcessor.EmitterInner<Integer> inner = new SinkManyEmitterProcessor.EmitterInner<>(null, processor);
		SinkManyEmitterProcessor.EmitterInner<Integer> notInner = new SinkManyEmitterProcessor.EmitterInner<>(null, processor);

		processor.add(inner);
		assertThat(processor.subscribers).as("adding inner").hasSize(1);

		processor.remove(notInner);
		assertThat(processor.subscribers).as("post remove notInner").hasSize(1);

		processor.remove(inner);
		assertThat(processor.subscribers).as("post remove inner").isEmpty();
	}

	@Test
	void currentContextDelegatesToFirstSubscriber() {
		AssertSubscriber<Object> testSubscriber1 = new AssertSubscriber<>(Context.of("key", "value1"));
		AssertSubscriber<Object> testSubscriber2 = new AssertSubscriber<>(Context.of("key", "value2"));

		SinkManyEmitterProcessor<Object> sinkManyEmitterProcessor = new SinkManyEmitterProcessor<>(false, 1);
		sinkManyEmitterProcessor.subscribe(testSubscriber1);
		sinkManyEmitterProcessor.subscribe(testSubscriber2);

		Context processorContext = sinkManyEmitterProcessor.currentContext();

		assertThat(processorContext.getOrDefault("key", "EMPTY")).isEqualTo("value1");
	}

	@Test
	void tryEmitNextWithNoSubscriberFailsOnlyIfNoCapacity() {
		SinkManyEmitterProcessor<Integer> sinkManyEmitterProcessor = new SinkManyEmitterProcessor<>(true, 1);

		assertThat(sinkManyEmitterProcessor.tryEmitNext(1)).isEqualTo(Sinks.EmitResult.OK);
		assertThat(sinkManyEmitterProcessor.tryEmitNext(2)).isEqualTo(Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER);

		StepVerifier.create(sinkManyEmitterProcessor)
		            .expectNext(1)
		            .then(() -> sinkManyEmitterProcessor.tryEmitComplete().orThrow())
		            .verifyComplete();
	}

	@Test
	void tryEmitNextWithNoSubscriberFailsIfNoCapacityAndAllSubscribersCancelledAndNoAutoTermination() {
		//in case of autoCancel, removing all subscribers results in TERMINATED rather than EMPTY
		SinkManyEmitterProcessor<Integer> sinkManyEmitterProcessor = new SinkManyEmitterProcessor<>(false, 1);
		AssertSubscriber<Integer> testSubscriber = AssertSubscriber.create();

		sinkManyEmitterProcessor.subscribe(testSubscriber);

		assertThat(sinkManyEmitterProcessor.tryEmitNext(1)).as("emit 1, with subscriber").isEqualTo(
				Sinks.EmitResult.OK);
		assertThat(sinkManyEmitterProcessor.tryEmitNext(2)).as("emit 2, with subscriber").isEqualTo(
				Sinks.EmitResult.OK);
		assertThat(sinkManyEmitterProcessor.tryEmitNext(3)).as("emit 3, with subscriber").isEqualTo(
				Sinks.EmitResult.OK);

		testSubscriber.cancel();

		assertThat(sinkManyEmitterProcessor.tryEmitNext(4)).as("emit 4, without subscriber, buffered").isEqualTo(
				Sinks.EmitResult.OK);
		assertThat(sinkManyEmitterProcessor.tryEmitNext(5)).as("emit 5, without subscriber").isEqualTo(
				Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER);
	}

	@Test
	void emitNextWithNoSubscriberNoCapacityKeepsSinkOpenWithBuffer() {
		SinkManyEmitterProcessor<Integer> sinkManyEmitterProcessor = new SinkManyEmitterProcessor<>(false, 1);
		//fill the buffer
		assertThat(sinkManyEmitterProcessor.tryEmitNext(1)).as("filling buffer").isEqualTo(Sinks.EmitResult.OK);
		//test proper
		//this is "discarded" but no hook can be invoked, so effectively dropped on the floor
		sinkManyEmitterProcessor.emitNext(2, FAIL_FAST);

		StepVerifier.create(sinkManyEmitterProcessor)
		            .expectNext(1)
		            .expectTimeout(Duration.ofSeconds(1))
		            .verify();
	}
}
