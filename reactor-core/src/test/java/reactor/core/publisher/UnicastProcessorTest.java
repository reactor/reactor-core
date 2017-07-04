/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.junit.Test;

import reactor.core.Disposable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.QueueSupplier;

import static org.junit.Assert.assertEquals;

public class UnicastProcessorTest {

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
		UnicastProcessor<Integer> processor = UnicastProcessor.create();
		FluxSink<Integer> sink = processor.sink();
		int nThreads = 5;
		int countPerThread = 10000;
		ExecutorService executor = Executors.newFixedThreadPool(nThreads);
		for (int i = 0; i < 5; i++) {
			Runnable generator = () -> {
				for (int j = 0; j < countPerThread; j++) {
					sink.next(j);
				}
			};
			executor.submit(generator);
		}
		StepVerifier.create(processor)
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
	@Deprecated
	public void createOverrideQueue() {
		Queue<Integer> queue = QueueSupplier.<Integer>get(10).get();
		UnicastProcessor<Integer> processor = UnicastProcessor.create(queue);
		assertProcessor(processor, queue, null, null);
	}

	@Test
	@Deprecated
	public void createOverrideQueueOnTerminate() {
		Disposable onTerminate = () -> {};
		Queue<Integer> queue = QueueSupplier.<Integer>get(10).get();
		UnicastProcessor<Integer> processor = UnicastProcessor.create(queue, onTerminate);
		assertProcessor(processor, queue, null, onTerminate);
	}

	@Test
	@Deprecated
	public void createOverrideAll() {
		Disposable onTerminate = () -> {};
		Consumer<? super Integer> onOverflow = t -> {};
		Queue<Integer> queue = QueueSupplier.<Integer>get(10).get();
		UnicastProcessor<Integer> processor = UnicastProcessor.create(queue, onOverflow, onTerminate);
		assertProcessor(processor, queue, onOverflow, onTerminate);
	}

	public void assertProcessor(UnicastProcessor<Integer> processor,
			@Nullable Queue<Integer> queue,
			@Nullable Consumer<? super Integer> onOverflow,
			@Nullable Disposable onTerminate) {
		Queue<Integer> expectedQueue = queue != null ? queue : QueueSupplier.<Integer>unbounded().get();
		Disposable expectedOnTerminate = onTerminate != null ? onTerminate : null;
		assertEquals(expectedQueue.getClass(), processor.queue.getClass());
		assertEquals(expectedOnTerminate, processor.onTerminate);
		if (onOverflow != null)
			assertEquals(onOverflow, processor.onOverflow);
	}
}
