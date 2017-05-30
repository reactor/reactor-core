/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.publisher.scenarios;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.logging.Level;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.WorkQueueProcessor;

import static org.testng.Assert.assertEquals;

/**
 * @author Michael Lex
 * @link https://github.com/reactor/reactor-core/issues/104
 */
public class BurstyWorkQueueProcessorTests {
	public static final int PRODUCER_LATENCY = 5;
	public static final int CONSUMER_LATENCY = 4;

	public static final int RINGBUFFER_SIZE = 128;

	public static final int INITAL_MESSAGES_COUNT   = 10;
	public static final int PRODUCED_MESSAGES_COUNT = 1024;
	public static final int BURST_SIZE              = 5;

	private LongAccumulator            maxRingBufferPending;
	private WorkQueueProcessor<Object> processor;
	private ExecutorService            producerExecutor;
	private AtomicLong                 droppedCount;

	@Before
	public void setup() {
		maxRingBufferPending =  new LongAccumulator(Long::max, Long.MIN_VALUE);
		droppedCount = new AtomicLong(0);
		producerExecutor = Executors.newSingleThreadExecutor();
	}

	@Test
	@Ignore
	public void test() throws Exception {
		processor = WorkQueueProcessor.builder().name("test-processor").bufferSize(RINGBUFFER_SIZE).build();

		Flux
				.create((emitter) -> burstyProducer(emitter, PRODUCED_MESSAGES_COUNT, BURST_SIZE))
				.onBackpressureDrop(this::incrementDroppedMessagesCounter)
			//	.log("test", Level.INFO, SignalType.REQUEST)
				.subscribeWith(processor)
				.map(this::complicatedCalculation)
				.subscribe(this::logConsumedValue);

		waitForProducerFinish();

		System.out.println("\n\nMax ringbuffer pending: " + maxRingBufferPending.get());

		assertEquals(getDroppedMessagesCount(), 0, "Expect zero dropped messages");
	}


	private void waitForProducerFinish() throws InterruptedException {
		producerExecutor.shutdown();
		producerExecutor.awaitTermination(20, TimeUnit.SECONDS);
	}

	private void logConsumedValue(Object value) {
		System.out.print(value + ",");
	}

	private long getDroppedMessagesCount() {
		return droppedCount.get();
	}

	private void incrementDroppedMessagesCounter(Object dropped) {
		System.out.println("\nDropped: " + dropped);
		droppedCount.incrementAndGet();
	}

	private Object complicatedCalculation(Object value) {
		maxRingBufferPending.accumulate(processor.getPending());
		sleep(CONSUMER_LATENCY);
		return value;
	}

	private void burstyProducer(FluxSink<Object> emitter, int messageCount, int burstSize) {
		producerExecutor.execute(burstyProducerRunnable(emitter, messageCount, burstSize));
	}

	public Runnable burstyProducerRunnable(final FluxSink<Object> emitter, int count, int
			burstSize) {
		return () -> {

			// Let's start with some messages to keep the ringbuffer scenario going total empty
			for (int i = 0; i < INITAL_MESSAGES_COUNT; ++i) {
				emitter.next("initial" + i);
			}

			for (int outer=0; outer<count/burstSize; ++outer) {
				for (int inner=0; inner<burstSize; ++inner) {
					emitter.next(outer*burstSize+inner);
				}
				sleep(PRODUCER_LATENCY * burstSize);
			}
		};
	}

	private static void sleep(int i) {
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
