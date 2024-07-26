/*
 * Copyright (c) 2015-2024 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.LockSupport;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.Scannable.Attr;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

// This is ok as this class tests the deprecated UnicastProcessor. Will be removed with it in 3.5.
@SuppressWarnings("deprecation")
public class SinkManyUnicastTest {

	@Test
	public void currentSubscriberCount() {
		Sinks.Many<Integer> sink = SinkManyUnicast.create();

		assertThat(sink.currentSubscriberCount()).isZero();

		sink.asFlux().subscribe();

		assertThat(sink.currentSubscriberCount()).isOne();
	}

    @Test
    public void secondSubscriberRejectedProperly() {

        SinkManyUnicast<Integer> up = SinkManyUnicast.create(new ConcurrentLinkedQueue<>());

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
		SinkManyUnicast<Integer> processor = SinkManyUnicast.create();
		assertProcessor(processor, null, null);
	}

	@Test
	public void createOverrideQueue() {
		Queue<Integer> queue = Queues.<Integer>get(10).get();
		SinkManyUnicast<Integer> processor = SinkManyUnicast.create(queue);
		assertProcessor(processor, queue, null);
	}

	@Test
	public void createOverrideQueueOnTerminate() {
		Disposable onTerminate = () -> {};
		Queue<Integer> queue = Queues.<Integer>get(10).get();
		SinkManyUnicast<Integer> processor = SinkManyUnicast.create(queue, onTerminate);
		assertProcessor(processor, queue, onTerminate);
	}

	void assertProcessor(SinkManyUnicast<Integer> processor,
								@Nullable Queue<Integer> queue,
								@Nullable Disposable onTerminate) {
		Queue<Integer> expectedQueue = queue != null ? queue : Queues.<Integer>unbounded().get();
		Disposable expectedOnTerminate = onTerminate;
		assertThat(processor.queue.getClass()).isEqualTo(expectedQueue.getClass());
		assertThat(processor.onTerminate).isEqualTo(expectedOnTerminate);
	}

	@Test
	void scanCapacityReactorUnboundedQueue() {
    	SinkManyUnicast processor = SinkManyUnicast.create(
    			Queues.unbounded(2).get());

    	assertThat(processor.scan(Attr.CAPACITY)).isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	void scanCapacityReactorBoundedQueue() {
		//the bounded queue floors at 8 and rounds to the next power of 2

		assertThat(SinkManyUnicast.create(Queues.get(2).get()).scan(Attr.CAPACITY))
				.isEqualTo(8);

		assertThat(SinkManyUnicast.create(Queues.get(8).get()).scan(Attr.CAPACITY))
				.isEqualTo(8);

		assertThat(SinkManyUnicast.create(Queues.get(9).get()).scan(Attr.CAPACITY))
				.isEqualTo(16);
	}

	@Test
	void scanCapacityBoundedBlockingQueue() {
		SinkManyUnicast processor = SinkManyUnicast.create(
				new LinkedBlockingQueue<>(10));

		assertThat(processor.scan(Attr.CAPACITY)).isEqualTo(10);
	}

	@Test
	void scanCapacityUnboundedBlockingQueue() {
		SinkManyUnicast processor = SinkManyUnicast.create(new LinkedBlockingQueue<>());

		assertThat(processor.scan(Attr.CAPACITY)).isEqualTo(Integer.MAX_VALUE);

	}

	@Test
	void scanCapacityOtherQueue() {
		SinkManyUnicast processor = SinkManyUnicast.create(new PriorityQueue<>(10));

		assertThat(processor.scan(Attr.CAPACITY))
				.isEqualTo(Integer.MIN_VALUE)
	            .isEqualTo(Queues.CAPACITY_UNSURE);
	}


	@Test
	public void contextTest() {
    	SinkManyUnicast<Integer> p = SinkManyUnicast.create();
    	p.contextWrite(ctx -> ctx.put("foo", "bar")).subscribe();

    	assertThat(p.currentContext().get("foo").toString()).isEqualTo("bar");
	}

	@Test
	public void subscriptionCancelUpdatesDownstreamCount() {
		SinkManyUnicast<String> processor = SinkManyUnicast.create();

		assertThat(processor.currentSubscriberCount())
				.as("before subscribe")
				.isZero();

		LambdaSubscriber<String> subscriber = new LambdaSubscriber<>(null, null, null, null);
		Disposable subscription = processor.subscribeWith(subscriber);

		assertThat(processor.currentSubscriberCount())
				.as("after subscribe")
				.isPositive();
		assertThat(processor.actual)
				.as("after subscribe has actual")
				.isSameAs(subscriber);

		subscription.dispose();

		assertThat(processor.currentSubscriberCount())
				.as("after subscription cancel")
				.isZero();
	}

	@Test
	public void shouldNotThrowFromTryEmitNext() {
		SinkManyUnicast<Object> processor = new SinkManyUnicast<>(Queues.empty().get());

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
		SinkManyUnicast<Object> processor = new SinkManyUnicast<>(Queues.empty().get());

		StepVerifier.create(processor, 0)
		            .expectSubscription()
		            .then(() -> processor.emitNext("boom", FAIL_FAST))
		            .verifyErrorMatches(Exceptions::isOverflow);
	}

	@Test
	public void tryEmitNextWithNoSubscriberAndBoundedQueueFailsZeroSubscriber() {
		SinkManyUnicast<Integer> sinkManyUnicast = SinkManyUnicast.create(Queues.<Integer>one().get());

		assertThat(sinkManyUnicast.tryEmitNext(1)).isEqualTo(Sinks.EmitResult.OK);
		assertThat(sinkManyUnicast.tryEmitNext(2)).isEqualTo(Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER);

		StepVerifier.create(sinkManyUnicast)
		            .expectNext(1)
		            .then(() -> sinkManyUnicast.tryEmitComplete().orThrow())
		            .verifyComplete();
	}

	@Test
	public void tryEmitNextWithBoundedQueueAndNoRequestFailsWithOverflow() {
		SinkManyUnicast<Integer> sinkManyUnicast = SinkManyUnicast.create(Queues.<Integer>one().get());

		StepVerifier.create(sinkManyUnicast, 0) //important to make no initial request
		            .expectSubscription()
		            .then(() -> {
			            assertThat(sinkManyUnicast.tryEmitNext(1)).isEqualTo(Sinks.EmitResult.OK);
			            assertThat(sinkManyUnicast.tryEmitNext(2)).isEqualTo(Sinks.EmitResult.FAIL_OVERFLOW);
			            assertThat(sinkManyUnicast.tryEmitComplete()).isEqualTo(Sinks.EmitResult.OK);
		            })
		            .thenRequest(1)
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void emitNextWithNoSubscriberAndBoundedQueueIgnoresValueAndKeepsSinkOpen() {
		SinkManyUnicast<Integer> sinkManyUnicast = SinkManyUnicast.create(Queues.<Integer>one().get());
		//fill the buffer
		sinkManyUnicast.tryEmitNext(1);
		//this "overflows" but keeps the sink open. since there's no subscriber, there's no Context so no real discarding
		sinkManyUnicast.emitNext(2, FAIL_FAST);

		//let's verify we get the buffer's content
		StepVerifier.create(sinkManyUnicast)
		            .expectNext(1) //from the buffer
		            .expectNoEvent(Duration.ofMillis(500))
		            .then(() -> sinkManyUnicast.tryEmitComplete().orThrow())
		            .verifyComplete();
	}

	@Test
	public void emitWithoutSubscriberAndSubscribeCancellingSubscriptionDiscards() {
		Sinks.Many<String> sink = Sinks.many()
		                               .unicast()
		                               .onBackpressureBuffer();

		sink.tryEmitNext("Hello");

		List<String> discarded = new CopyOnWriteArrayList<String>();

		sink.asFlux()
		    .doOnSubscribe(Subscription::cancel)
		    .contextWrite(ctx -> Operators.enableOnDiscard(ctx,
				               item -> discarded.add((String) item)))
		    .subscribe();

		assertThat(discarded).containsExactly("Hello");
	}

	@Test
	public void scanTerminatedCancelled() {
		Sinks.Many<Integer> sink = SinkManyUnicast.create();

		assertThat(sink.scan(Attr.TERMINATED)).as("not yet terminated").isFalse();

		sink.tryEmitError(new IllegalStateException("boom")).orThrow();

		assertThat(sink.scan(Attr.TERMINATED)).as("terminated with error").isTrue();
		assertThat(sink.scan(Attr.ERROR)).as("error").hasMessage("boom");

		assertThat(sink.scan(Attr.CANCELLED)).as("pre-cancellation").isFalse();

		((SinkManyUnicast<?>) sink).cancel();

		assertThat(sink.scan(Attr.CANCELLED)).as("cancelled").isTrue();
	}

	@Test
	public void inners() {
		Sinks.Many<Integer> sink1 = SinkManyUnicast.create();
		Sinks.Many<Integer> sink2 = SinkManyUnicast.create();
		CoreSubscriber<Integer> notScannable = new BaseSubscriber<Integer>() {};
		InnerConsumer<Integer> scannable = new LambdaSubscriber<>(null, null, null, null);

		assertThat(sink1.inners()).as("before subscription notScannable").isEmpty();
		assertThat(sink2.inners()).as("before subscription notScannable").isEmpty();

		sink1.asFlux().subscribe(notScannable);
		sink2.asFlux().subscribe(scannable);

		assertThat(sink1.inners())
				.asInstanceOf(InstanceOfAssertFactories.LIST)
				.as("after notScannable subscription")
				.containsExactly(Scannable.from("NOT SCANNABLE"));

		assertThat(sink2.inners())
				.asInstanceOf(InstanceOfAssertFactories.LIST)
				.as("after scannable subscription")
				.containsExactly(scannable);
	}
}
