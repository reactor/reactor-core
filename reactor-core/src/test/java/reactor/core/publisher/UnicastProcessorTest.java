/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.time.Duration;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.MemoryUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

// This is ok as this class tests the deprecated UnicastProcessor. Will be removed with it in 3.5.
@SuppressWarnings("deprecation")
public class UnicastProcessorTest {

	@Test
	public void currentSubscriberCount() {
		Sinks.Many<Integer> sink = UnicastProcessor.create();

		assertThat(sink.currentSubscriberCount()).isZero();

		sink.asFlux().subscribe();

		assertThat(sink.currentSubscriberCount()).isOne();
	}

    @Test
    public void secondSubscriberRejectedProperly() {

        UnicastProcessor<Integer> up = UnicastProcessor.create(new ConcurrentLinkedQueue<>());

        up.subscribe();

        AssertSubscriber<Integer> ts = AssertSubscriber.create();

        up.subscribe(ts);

        ts.assertNoValues()
        .assertError(IllegalStateException.class)
        .assertNotComplete();

    }

	@Test
	public void multiThreadedProducer() {
		Sinks.Many<Integer> sink = Sinks.many().unicast().onBackpressureBuffer();
		int nThreads = 5;
		int countPerThread = 10000;
		ExecutorService executor = Executors.newFixedThreadPool(nThreads);
		for (int i = 0; i < 5; i++) {
			Runnable generator = () -> {
				for (int j = 0; j < countPerThread; j++) {
					while (sink.tryEmitNext(j).isFailure()) {
						LockSupport.parkNanos(10);
					}
				}
			};
			executor.submit(generator);
		}
		StepVerifier.create(sink.asFlux())
					.expectNextCount(nThreads * countPerThread)
					.thenCancel()
					.verify();
		executor.shutdownNow();
	}

	@Test
	public void createDefault() {
		UnicastProcessor<Integer> processor = UnicastProcessor.create();
		assertProcessor(processor, null, null, null);
	}

	@Test
	public void createOverrideQueue() {
		Queue<Integer> queue = Queues.<Integer>get(10).get();
		UnicastProcessor<Integer> processor = UnicastProcessor.create(queue);
		assertProcessor(processor, queue, null, null);
	}

	@Test
	public void createOverrideQueueOnTerminate() {
		Disposable onTerminate = () -> {};
		Queue<Integer> queue = Queues.<Integer>get(10).get();
		UnicastProcessor<Integer> processor = UnicastProcessor.create(queue, onTerminate);
		assertProcessor(processor, queue, null, onTerminate);
	}

	@Test
	public void createOverrideAll() {
		Disposable onTerminate = () -> {};
		Consumer<? super Integer> onOverflow = t -> {};
		Queue<Integer> queue = Queues.<Integer>get(10).get();
		UnicastProcessor<Integer> processor = UnicastProcessor.create(queue, onOverflow, onTerminate);
		assertProcessor(processor, queue, onOverflow, onTerminate);
	}

	public void assertProcessor(UnicastProcessor<Integer> processor,
			@Nullable Queue<Integer> queue,
			@Nullable Consumer<? super Integer> onOverflow,
			@Nullable Disposable onTerminate) {
		Queue<Integer> expectedQueue = queue != null ? queue : Queues.<Integer>unbounded().get();
		Disposable expectedOnTerminate = onTerminate;
		assertThat(processor.queue.getClass()).isEqualTo(expectedQueue.getClass());
		assertThat(processor.onTerminate).isEqualTo(expectedOnTerminate);
		if (onOverflow != null)
			assertThat(processor.onOverflow).isEqualTo(onOverflow);
	}

	@Test
	public void bufferSizeReactorUnboundedQueue() {
    	UnicastProcessor processor = UnicastProcessor.create(
    			Queues.unbounded(2).get());

    	assertThat(processor.getBufferSize()).isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	public void bufferSizeReactorBoundedQueue() {
    	//the bounded queue floors at 8 and rounds to the next power of 2

		assertThat(UnicastProcessor.create(Queues.get(2).get())
		                           .getBufferSize())
				.isEqualTo(8);

		assertThat(UnicastProcessor.create(Queues.get(8).get())
		                           .getBufferSize())
				.isEqualTo(8);

		assertThat(UnicastProcessor.create(Queues.get(9).get())
		                           .getBufferSize())
				.isEqualTo(16);
	}

	@Test
	public void bufferSizeBoundedBlockingQueue() {
		UnicastProcessor processor = UnicastProcessor.create(
				new LinkedBlockingQueue<>(10));

		assertThat(processor.getBufferSize()).isEqualTo(10);
	}

	@Test
	public void bufferSizeUnboundedBlockingQueue() {
		UnicastProcessor processor = UnicastProcessor.create(
				new LinkedBlockingQueue<>());

		assertThat(processor.getBufferSize()).isEqualTo(Integer.MAX_VALUE);

	}

	@Test
	public void bufferSizeOtherQueue() {
		Sinks.Many<?> processor = Sinks.many().unicast().onBackpressureBuffer(
				new PriorityQueue<>(10));

		assertThat(Scannable.from(processor).scan(Scannable.Attr.CAPACITY))
				.isEqualTo(Integer.MIN_VALUE)
	            .isEqualTo(Queues.CAPACITY_UNSURE);
	}


	@Test
	public void contextTest() {
    	UnicastProcessor<Integer> p = UnicastProcessor.create();
    	p.contextWrite(ctx -> ctx.put("foo", "bar")).subscribe();

    	assertThat(p.sink().currentContext().get("foo").toString()).isEqualTo("bar");
	}

	@Test
	public void subscriptionCancelUpdatesDownstreamCount() {
		UnicastProcessor<String> processor = UnicastProcessor.create();

		assertThat(processor.currentSubscriberCount())
				.as("before subscribe")
				.isZero();

		LambdaSubscriber<String> subscriber = new LambdaSubscriber<>(null, null, null, null);
		Disposable subscription = processor.subscribeWith(subscriber);

		assertThat(processor.currentSubscriberCount())
				.as("after subscribe")
				.isPositive();
		assertThat(processor.actual())
				.as("after subscribe has actual")
				.isSameAs(subscriber);

		subscription.dispose();

		assertThat(processor.currentSubscriberCount())
				.as("after subscription cancel")
				.isZero();
	}

	@Test
	public void ensureNoLeaksIfRacingDisposeAndOnNext() {
		Hooks.onNextDropped(MemoryUtils.Tracked::safeRelease);
		try {
			MemoryUtils.OffHeapDetector tracker = new MemoryUtils.OffHeapDetector();
			for (int i = 0; i < 10000; i++) {
				tracker.reset();
				TestPublisher<MemoryUtils.Tracked> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION,
						TestPublisher.Violation.REQUEST_OVERFLOW);
				UnicastProcessor<MemoryUtils.Tracked> unicastProcessor = UnicastProcessor.create();

				testPublisher.subscribe(unicastProcessor);

				AssertSubscriber<MemoryUtils.Tracked> assertSubscriber =
						new AssertSubscriber<>(Operators.enableOnDiscard(null, MemoryUtils.Tracked::safeRelease));

				unicastProcessor.subscribe(assertSubscriber);

				testPublisher.next(tracker.track(1));
				testPublisher.next(tracker.track(2));

				MemoryUtils.Tracked value3 = tracker.track(3);
				MemoryUtils.Tracked value4 = tracker.track(4);
				MemoryUtils.Tracked value5 = tracker.track(5);

				RaceTestUtils.race(unicastProcessor::dispose, () -> {
					testPublisher.next(value3);
					testPublisher.next(value4);
					testPublisher.next(value5);
				});

				assertSubscriber.assertTerminated()
				                .assertError(CancellationException.class)
				                .assertErrorMessage("Disposed");

				List<MemoryUtils.Tracked> values = assertSubscriber.values();
				values.forEach(MemoryUtils.Tracked::release);

				tracker.assertNoLeaks();
			}
		} finally {
			Hooks.resetOnNextDropped();
		}
	}

	@Test
	public void shouldNotThrowFromTryEmitNext() {
		UnicastProcessor<Object> processor = new UnicastProcessor<>(Queues.empty().get());

		StepVerifier.create(processor, 0)
		            .expectSubscription()
		            .then(() -> {
			            assertThat(processor.tryEmitNext("boom"))
					            .as("emission")
					            .isEqualTo(Sinks.EmitResult.FAIL_OVERFLOW);
		            })
		            .then(() -> processor.tryEmitComplete().orThrow())
		            .verifyComplete();
	}

	@Test
	public void shouldSignalErrorOnOverflow() {
		UnicastProcessor<Object> processor = new UnicastProcessor<>(Queues.empty().get());

		StepVerifier.create(processor, 0)
		            .expectSubscription()
		            .then(() -> processor.emitNext("boom", FAIL_FAST))
		            .verifyErrorMatches(Exceptions::isOverflow);
	}

	@Test
	public void tryEmitNextWithNoSubscriberAndBoundedQueueFailsZeroSubscriber() {
		UnicastProcessor<Integer> unicastProcessor = UnicastProcessor.create(Queues.<Integer>one().get());

		assertThat(unicastProcessor.tryEmitNext(1)).isEqualTo(Sinks.EmitResult.OK);
		assertThat(unicastProcessor.tryEmitNext(2)).isEqualTo(Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER);

		StepVerifier.create(unicastProcessor)
		            .expectNext(1)
		            .then(() -> unicastProcessor.tryEmitComplete().orThrow())
		            .verifyComplete();
	}

	@Test
	public void tryEmitNextWithBoundedQueueAndNoRequestFailsWithOverflow() {
		UnicastProcessor<Integer> unicastProcessor = UnicastProcessor.create(Queues.<Integer>one().get());

		StepVerifier.create(unicastProcessor, 0) //important to make no initial request
		            .expectSubscription()
		            .then(() -> {
			            assertThat(unicastProcessor.tryEmitNext(1)).isEqualTo(Sinks.EmitResult.OK);
			            assertThat(unicastProcessor.tryEmitNext(2)).isEqualTo(Sinks.EmitResult.FAIL_OVERFLOW);
			            assertThat(unicastProcessor.tryEmitComplete()).isEqualTo(Sinks.EmitResult.OK);
		            })
		            .thenRequest(1)
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void emitNextWithNoSubscriberAndBoundedQueueIgnoresValueAndKeepsSinkOpen() {
		UnicastProcessor<Integer> unicastProcessor = UnicastProcessor.create(Queues.<Integer>one().get());
		//fill the buffer
		unicastProcessor.tryEmitNext(1);
		//this "overflows" but keeps the sink open. since there's no subscriber, there's no Context so no real discarding
		unicastProcessor.emitNext(2, FAIL_FAST);

		//let's verify we get the buffer's content
		StepVerifier.create(unicastProcessor)
		            .expectNext(1) //from the buffer
		            .expectNoEvent(Duration.ofMillis(500))
		            .then(() -> unicastProcessor.tryEmitComplete().orThrow())
		            .verifyComplete();
	}

	@Test //TODO that onOverflow API isn't exposed via Sinks. But maybe it should be generalized?
	public void emitNextWithNoSubscriberAndBoundedQueueAndHandlerHandlesValueAndKeepsSinkOpen() {
		Disposable sinkDisposed = Disposables.single();
		List<Integer> discarded = new CopyOnWriteArrayList<>();
		UnicastProcessor<Integer> unicastProcessor = UnicastProcessor.create(Queues.<Integer>one().get(),
				discarded::add, sinkDisposed);
		//fill the buffer
		unicastProcessor.tryEmitNext(1);
		//this "overflows" but keeps the sink open
		unicastProcessor.emitNext(2, FAIL_FAST);

		assertThat(discarded).containsExactly(2);
		assertThat(sinkDisposed.isDisposed()).as("sinkDisposed").isFalse();

		unicastProcessor.emitComplete(FAIL_FAST);

		//let's verify we get the buffer's content
		StepVerifier.create(unicastProcessor)
		            .expectNext(1) //from the buffer
		            .verifyComplete();
	}

	@Test
	public void scanTerminatedCancelled() {
		Sinks.Many<Integer> sink = UnicastProcessor.create();

		assertThat(sink.scan(Scannable.Attr.TERMINATED)).as("not yet terminated").isFalse();

		sink.tryEmitError(new IllegalStateException("boom")).orThrow();

		assertThat(sink.scan(Scannable.Attr.TERMINATED)).as("terminated with error").isTrue();
		assertThat(sink.scan(Scannable.Attr.ERROR)).as("error").hasMessage("boom");

		assertThat(sink.scan(Scannable.Attr.CANCELLED)).as("pre-cancellation").isFalse();

		((UnicastProcessor<?>) sink).cancel();

		assertThat(sink.scan(Scannable.Attr.CANCELLED)).as("cancelled").isTrue();
	}

	@Test
	public void inners() {
		Sinks.Many<Integer> sink1 = UnicastProcessor.create();
		Sinks.Many<Integer> sink2 = UnicastProcessor.create();
		CoreSubscriber<Integer> notScannable = new BaseSubscriber<Integer>() {};
		InnerConsumer<Integer> scannable = new LambdaSubscriber<>(null, null, null, null);

		assertThat(sink1.inners()).as("before subscription notScannable").isEmpty();
		assertThat(sink2.inners()).as("before subscription notScannable").isEmpty();

		sink1.asFlux().subscribe(notScannable);
		sink2.asFlux().subscribe(scannable);

		assertThat(sink1.inners())
				.asList()
				.as("after notScannable subscription")
				.containsExactly(Scannable.from("NOT SCANNABLE"));

		assertThat(sink2.inners())
				.asList()
				.as("after scannable subscription")
				.containsExactly(scannable);
	}
}
