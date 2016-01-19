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

package reactor.core.publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Flux;
import reactor.Processors;
import reactor.core.subscriber.test.DataTestSubscriber;
import reactor.core.subscriber.test.TestSubscriber;
import reactor.core.support.BackpressureUtils;

/**
 * ?
 *
 * @author Anatoly Kadyshev
 */
public class EmitterProcessorDemandTests {

	static final List<String> DATA     = new ArrayList<>();
	static final int          MAX_SIZE = 100;

	static {
		for (int i = 1; i <= MAX_SIZE; i++) {
			DATA.add("" + i);
		}
	}

	@Test
	@Ignore
	public void test() throws InterruptedException {
		ProcessorGroup<String> asyncGroup = Processors.asyncGroup("async", 128, 1);
		FluxProcessor<String, String> publishOn = asyncGroup.publishOn();
		FluxProcessor<String, String> emitter = Processors.emitter();

		publishOn.subscribe(emitter);

		CountDownLatch requestReceived = new CountDownLatch(1);
		AtomicLong demand = new AtomicLong(0);
		Publisher<String> publisher = s -> s.onSubscribe(new Subscription() {
			@Override
			public void request(long n) {
				System.out.println("request: " + n + " " + s);
				demand.addAndGet(n);
				requestReceived.countDown();
			}

			@Override
			public void cancel() {
				System.out.println("cancel");
			}
		});

		publisher.subscribe(publishOn);

		TestSubscriber<String> subscriber = TestSubscriber.createWithTimeoutSecs(1);
		emitter.subscribe(subscriber);

		subscriber.request(Long.MAX_VALUE);

		if (!requestReceived.await(1, TimeUnit.SECONDS)) {
			throw new RuntimeException();
		}

		int i = 0;
		for (; ; ) {
			if (BackpressureUtils.getAndSub(demand, 1) != 0) {
				publishOn.onNext("" + (i++));
			}
			else {
				System.out.println("NO REQUESTED: " + publishOn + " " + emitter);
				LockSupport.parkNanos(100_000_000);
			}
		}
	}

	@Test
	@Ignore
	public void testPerformance() throws InterruptedException {
		FluxProcessor<String, String> emitter = Processors.emitter();

		CountDownLatch requestReceived = new CountDownLatch(1);

		AtomicLong maxDelay = new AtomicLong(0);
		AtomicLong demand = new AtomicLong(0);
		Publisher<String> publisher = new Publisher<String>() {

			long lastTimeRequestReceivedNs = -1;

			@Override
			public void subscribe(Subscriber<? super String> s) {
				s.onSubscribe(new Subscription() {
					@Override
					public void request(long n) {
						requestReceived.countDown();

						long now = System.nanoTime();

						if (lastTimeRequestReceivedNs > 0) {
							maxDelay.set(now - lastTimeRequestReceivedNs);
						}

						lastTimeRequestReceivedNs = now;

						demand.addAndGet(n);
					}

					@Override
					public void cancel() {
						System.out.println("cancel");
					}
				});
			}
		};

		publisher.subscribe(emitter);

		TestSubscriber<String> subscriber = TestSubscriber.createWithTimeoutSecs(1);
		emitter.subscribe(subscriber);
		subscriber.requestUnboundedWithTimeout();

		if (!requestReceived.await(1, TimeUnit.SECONDS)) {
			throw new RuntimeException();
		}

		String buffer = "Hello";
		int i = 0;
		for (; ; ) {
			if (BackpressureUtils.getAndSub(demand, 1) > 0) {
				emitter.onNext(buffer);
			}

			if (i++ % 1000000 == 0) {
				System.out.println("maxDelay: " + TimeUnit.MICROSECONDS.toMillis(maxDelay.get()) + " µs");
			}
		}
	}

	@Test
	public void testRed() throws InterruptedException {
		FluxProcessor<String, String> processor = Processors.emitter();
		DataTestSubscriber<String> subscriber = DataTestSubscriber.createWithTimeoutSecs(1);
		processor.subscribe(subscriber);

		subscriber.request(1);
		Flux.fromIterable(DATA)
		    .log()
		    .subscribe(processor);

		subscriber.assertNextSignals("1");
	}

	@Test
	public void testGreen() throws InterruptedException {
		FluxProcessor<String, String> processor = Processors.emitter();
		DataTestSubscriber<String> subscriber = DataTestSubscriber.createWithTimeoutSecs(1);
		processor.subscribe(subscriber);

		Flux.fromIterable(DATA)
		    .log()
		    .subscribe(processor);

		subscriber.request(1);

		subscriber.assertNextSignals("1");
	}

	@Test
	public void testHanging() throws InterruptedException {
		FluxProcessor<String, String> processor = Processors.emitter(2);
		Flux.fromIterable(DATA)
		    .log()
		    .subscribe(processor);

		DataTestSubscriber<String> first = DataTestSubscriber.createWithTimeoutSecs(1);
		processor.log("after-1")
		         .subscribe(first);

		DataTestSubscriber<String> second = DataTestSubscriber.createWithTimeoutSecs(1);
		processor.log("after-2")
		         .subscribe(second);

		second.request(1);
		second.assertNextSignals("1");

		first.request(3);
		first.assertNextSignals("1", "2", "3");
	}

	@Test
	public void testNPE() throws InterruptedException {
		FluxProcessor<String, String> processor = Processors.emitter(8);
		Flux.fromIterable(DATA)
		    .log()
		    .subscribe(processor);

		DataTestSubscriber<String> first = DataTestSubscriber.createWithTimeoutSecs(1);
		processor.subscribe(first);

		first.request(1);
		first.assertNextSignals("1");

		DataTestSubscriber<String> second = DataTestSubscriber.createWithTimeoutSecs(1);
		processor.subscribe(second);

		second.request(3);
		second.assertNextSignals("2", "3", "4");
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

		public MyThread(FluxProcessor<String, String> processor, CyclicBarrier barrier, int n, int index) {
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
			TestSubscriber<String> subscriber = TestSubscriber.createWithTimeoutSecs(5);
			processor.subscribe(subscriber);
			barrier.await();

			subscriber.request(3);
			subscriber.request(4);
			subscriber.request(1);

			try{
				subscriber
						.assertNumNextSignalsReceived(n)
						.assertCompleteReceived();
			}
			finally {
				System.out.println(subscriber.debug());
			}
		}

		public Throwable getLastException() {
			return lastException;
		}

	}

	@Test
	@Ignore
	public void testRacing() throws Exception {
		int N_THREADS = 3;
		int N_ITEMS = 8;

		FluxProcessor<String, String> processor = Processors.emitter(4);
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
				Assert.fail();
			}
		}
	}

}