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

package reactor.core.publisher.tck;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.publisher.BlockingSink;
import reactor.core.publisher.Flux;
import reactor.core.publisher.TopicProcessor;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.TimedScheduler;
import reactor.test.TestSubscriber;
import reactor.util.Logger;
import reactor.util.Loggers;

import static reactor.util.concurrent.WaitStrategy.liteBlocking;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class WorkQueueProcessorTests extends AbstractProcessorVerification {

	@Override
	public Processor<Long, Long> createProcessor(int bufferSize) {
		System.out.println("new processor");
		return  WorkQueueProcessor.create("rb-work", bufferSize);
	}

	@Override
	public void required_mustRequestFromUpstreamForElementsThatHaveBeenRequestedLongAgo()
			throws Throwable {
		//IGNORE since subscribers see distinct data
	}

	@Override
	public void required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError()
			throws Throwable {
		super.required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError();
	}

	@Test
	public void drainTest() throws Exception {
		final TopicProcessor<Integer> sink = TopicProcessor.create("topic");
		sink.onNext(1);
		sink.onNext(2);
		sink.onNext(3);

		TestSubscriber.subscribe(sink.forceShutdown())
		                             .assertComplete()
		                             .assertValues(1, 2, 3);
	}

	@Override
	public void simpleTest() throws Exception {
		final TopicProcessor<Integer> sink = TopicProcessor.create("topic");
		final WorkQueueProcessor<Integer> processor = WorkQueueProcessor.create("queue");

		int elems = 1_000_000;
		CountDownLatch latch = new CountDownLatch(elems);

		//List<Integer> list = new CopyOnWriteArrayList<>();
		AtomicLong count = new AtomicLong();
		AtomicLong errorCount = new AtomicLong();

		processor.subscribe(d -> {
			errorCount.incrementAndGet();
			throw Exceptions.failWithCancel();
		});

		Flux.from(processor).doOnNext(
			d -> count.incrementAndGet()
		).subscribe(d -> {
			latch.countDown();
			//list.add(d);
		});

		sink.subscribe(processor);
		sink.connect();
		for(int i = 0; i < elems; i++){

			sink.onNext(i);
			if( i % 100 == 0) {
				processor.subscribe(d -> {
					errorCount.incrementAndGet();
					throw Exceptions.failWithCancel();
				});
			}
		}

		latch.await(5, TimeUnit.SECONDS);
		System.out.println("count " + count+" errors: "+errorCount);
		sink.onComplete();
		Assert.assertTrue("Latch is " + latch.getCount(), latch.getCount() <= 1);
	}

	@Override
	public void mustImmediatelyPassOnOnErrorEventsReceivedFromItsUpstreamToItsDownstream() throws Exception {
		super.mustImmediatelyPassOnOnErrorEventsReceivedFromItsUpstreamToItsDownstream();
	}


	public static final Logger logger = Loggers.getLogger(WorkQueueProcessorTests.class);

	public static final String e = "Element";
	public static final String s = "Synchronizer";


	@Test
	public void highRate() throws Exception {
		WorkQueueProcessor<String> queueProcessor = WorkQueueProcessor.share("Processor", 256, liteBlocking());
		TimedScheduler timer = Schedulers.newTimer("Timer");
		queueProcessor
				.bufferMillis(32, 2, timer)
				.subscribe(new Subscriber<List<String>>() {
					int counter;
					@Override
					public void onSubscribe(Subscription s) {
						s.request(Long.MAX_VALUE);
					}

					@Override
					public void onNext(List<String> strings) {
						int size = strings.size();
						counter += size;
						if (strings.contains(s)) {
							synchronized (s) {
								//logger.debug("Synchronizer!");
								s.notifyAll();
							}
						}
					}

					@Override
					public void onError(Throwable t) {
						t.printStackTrace();
					}

					@Override
					public void onComplete() {
						System.out.println("Consumed in total: " + counter);
					}
				});
		BlockingSink<String> emitter = queueProcessor.connectSink();

		try {
			submitInCurrentThread(emitter);
		} finally {
			logger.debug("Finishing");
			emitter.finish();
			timer.shutdown();
		}
		TimeUnit.SECONDS.sleep(1);
	}

	public static void submitInCurrentThread(BlockingSink<String> emitter) {
		Random rand = new Random();
		for (int i = 0; i < 1000; i++) {
			long re = emitter.submit(e);
			logger.debug("Submit element result " + re);
			LockSupport.parkNanos(2_000_000 + rand.nextInt(200_000) - 100_000);
			synchronized (s) {
				long rd = emitter.submit(s);
				logger.debug("Submit drain result " + rd);
				timeoutWait(s);
			}
		}
	}

	private static void timeoutWait(Object o) {
		long t0 = System.currentTimeMillis();
		try {
			o.wait(5_000);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		if (System.currentTimeMillis() - t0 > 4_000) {
			throw new RuntimeException("Timeout!");
		}
	}

}
