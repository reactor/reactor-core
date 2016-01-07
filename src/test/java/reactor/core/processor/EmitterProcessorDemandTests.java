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

package reactor.core.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

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
		for (int i = 1; i < MAX_SIZE; i++) {
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

}