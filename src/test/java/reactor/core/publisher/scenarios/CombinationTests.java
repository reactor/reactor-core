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

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.flow.Cancellation;
import reactor.core.publisher.Computations;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.publisher.TopicProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.subscriber.SignalEmitter;
import reactor.core.subscriber.Subscribers;
import reactor.core.test.TestSubscriber;
import reactor.core.util.Logger;
import reactor.core.util.ReactiveStateUtils;

/**
 * @author Stephane Maldini
 */
public class CombinationTests {

	private static final Logger LOG = Logger.getLogger(CombinationTests.class);

	private FluxProcessor<SensorData, SensorData> sensorEven;
	private FluxProcessor<SensorData, SensorData> sensorOdd;

	@Before
	public void before() {
		sensorEven();
		sensorOdd();
	}

	@After
	public void then() {
		if (sensorEven != null) {
			sensorEven.onComplete();
			sensorEven = null;
		}
		if (sensorOdd != null) {
			sensorOdd.onComplete();
			sensorOdd = null;
		}
	}

	public Consumer<Object> loggingConsumer() {
		return m -> LOG.info("(int) msg={}", m);
	}
/*
	@Test
	public void testMerge1ToN() throws Exception {
		final int n = 1000;

		Flux<Integer> stream = Flux.merge(Flux.map(Flux.just(1), i -> Flux.range(0, n)));

		final CountDownLatch latch = new CountDownLatch(n);
		awaitLatch(stream.subscribe(integer -> latch.countDown()), latch);
	}*/

	@Test
	public void tesSubmitSession() throws Exception {
		FluxProcessor<Integer, Integer> processor = EmitterProcessor.create();
		AtomicInteger count = new AtomicInteger();
		CountDownLatch latch = new CountDownLatch(1);
		processor.publishOn(Computations.concurrent())
		         .subscribe(Subscribers.create(s -> {
			         try {
				         System.out.println("test" + Thread.currentThread());
				         Thread.sleep(1000);
			         }
			         catch (InterruptedException ie) {
				         //IGNORE
			         }
			         s.request(1L);
			         return null;
		         }, (d, s) -> {
			         count.incrementAndGet();
			         latch.countDown();
		         }));

		SignalEmitter<Integer> session = processor.connectEmitter();
		long emission = session.submit(1);
		if (emission == -1L) {
			throw new IllegalStateException("Negatime " + emission);
		}
		//System.out.println(emission);
		if (session.hasFailed()) {
			session.getError()
			       .printStackTrace();
		}
		session.finish();

		latch.await(5, TimeUnit.SECONDS);
		Assert.assertTrue("latch : " + count, count.get() == 1);
		Assert.assertTrue("time : " + emission, emission >= 0);
	}

	@Test
	public void testEmitter() throws Throwable {
		FluxProcessor<Integer, Integer> processor = EmitterProcessor.create();

		int n = 100_000;
		int subs = 4;
		final CountDownLatch latch = new CountDownLatch((n + 1) * subs);
		Scheduler c = Computations.single();
		for (int i = 0; i < subs; i++) {
			processor.publishOn(c)
			         .subscribe(Subscribers.create(s -> {
				         s.request(1L);
				         return null;
			         }, (d, s) -> {
//				         System.out.println(d);
				         s.request(1L);
				         latch.countDown();
			         }, null, d -> latch.countDown()));
		}

		SignalEmitter<Integer> session = processor.connectEmitter();

		for (int i = 0; i < n; i++) {
			while (!session.emit(i)
			               .isOk()) {
				//System.out.println(emission);
				if (session.hasFailed()) {
					session.getError()
					       .printStackTrace();
					throw session.getError();
				}
			}
		}
		session.finish();

		boolean waited = latch.await(5, TimeUnit.SECONDS);
		Assert.assertTrue( "latch : " + latch.getCount(), waited);
	}

	public Flux<SensorData> sensorOdd() {
		if (sensorOdd == null) {
			// this is the stream we publish odd-numbered events to
			this.sensorOdd = TopicProcessor.create("odd");

			// add substream to "master" list
			//allSensors().add(sensorOdd.reduce(this::computeMin).timeout(1000));
		}

		return sensorOdd.log("odd");
	}

	public Flux<SensorData> sensorEven() {
		if (sensorEven == null) {
			// this is the stream we publish even-numbered events to
			this.sensorEven = TopicProcessor.create("even");

			// add substream to "master" list
			//allSensors().add(sensorEven.reduce(this::computeMin).timeout(1000));
		}
		return sensorEven.log("even");
	}

	@Test
	public void sampleMergeTest() throws Exception {
		int elements = 40;
		CountDownLatch latch = new CountDownLatch(elements + 1);

		Publisher<SensorData> p = Flux.merge(sensorOdd(), sensorEven())
		                              .log("merge");

		generateData(elements);

		awaitLatch(p, latch);
	}

	@Test
	public void sampleAmbTest() throws Exception {
		int elements = 40;
		CountDownLatch latch = new CountDownLatch(elements / 2 + 1);

		Publisher<SensorData> p = Flux.amb(sensorOdd(), sensorEven())
		                              .log("amb");

		System.out.println(ReactiveStateUtils.scan(p)
		                                     .toString());

		Subscriber<SensorData> s = Subscribers.unbounded((d, sub) -> latch.countDown(), null, n -> latch.countDown());
		p.subscribe(s);
		Thread.sleep(1000);
		System.out.println(ReactiveStateUtils.scan(s)
		                                     .toString());

		generateData(elements);
	}

	/*@Test
	public void sampleConcatTestConsistent() throws Exception {
		for(int i = 0; i < 1000; i++){
			System.out.println("------");
			sampleConcatTest();
		}
	}*/

	@Test
	public void sampleConcatTest() throws Exception {
		int elements = 40;

		CountDownLatch latch = new CountDownLatch(elements + 1);

		Publisher<SensorData> p = Flux.concat(sensorEven(), sensorOdd())
		                              .log("concat");

		//System.out.println(tail.debug());
		generateData(elements);

		awaitLatch(p, latch);
	}

	@Test
	public void sampleZipTest() throws Exception {
		int elements = 69;
		CountDownLatch latch = new CountDownLatch((elements / 2) + 1);

		Publisher<SensorData> p = Flux.zip(sensorEven(), sensorOdd(), this::computeMin)
		                              .log("zip");

		generateData(elements);

		awaitLatch(p, latch);
	}

	@Test
	public void sampleMergeMonoTest() throws Exception {
		CountDownLatch latch = new CountDownLatch(2);

		Flux<Integer> p = Flux.merge(Flux.<Integer>empty().next(), Mono.just(1))
		                              .log("mono");

		awaitLatch(p, latch);
	}

	@Test
	public void sampleZipTest2() throws Exception {
		int elements = 1;
		CountDownLatch latch = new CountDownLatch(elements + 1);

		Publisher<SensorData> p = Flux.zip(sensorEven(), Flux.just(new SensorData(1L, 14.0f)), this::computeMin)
		                              .log("zip2");

		generateData(elements);

		awaitLatch(p, latch);
	}

	@Test
	public void sampleZipTest3() throws Exception {
		int elements = 1;
		CountDownLatch latch = new CountDownLatch(elements + 1);
		EmitterProcessor<SensorData> sensorDataProcessor = EmitterProcessor.create();

		sensorDataProcessor.publishOn(Computations.single())
		                   .subscribe(Subscribers.unbounded((d, sub) -> latch.countDown(),
				null,
				n -> latch.countDown()));

		Flux.zip(Flux.just(new SensorData(2L, 12.0f)), Flux.just(new SensorData(1L, 14.0f)), this::computeMin)
		    .log("zip3")
		    .subscribe(sensorDataProcessor);

		System.out.println(ReactiveStateUtils.scan(sensorDataProcessor)
		                                     .toString());

		awaitLatch(null, latch);
	}

	@SuppressWarnings("unchecked")
	private void awaitLatch(Publisher<?> tail, CountDownLatch latch) throws Exception {
		if (tail != null) {
			tail.subscribe(Subscribers.unbounded((d, sub) -> latch.countDown(), null, n -> latch.countDown()));
		}
		if (!latch.await(10, TimeUnit.SECONDS)) {
			throw new Exception("Never completed: (" + latch.getCount() + ") ");
		}
	}

	private void generateData(int elements) {
		Random random = new Random();
		SensorData data;
		Subscriber<SensorData> upstream;

		for (long i = 0; i < elements; i++) {
			data = new SensorData(i, random.nextFloat() * 100);
			if (i % 2 == 0) {
				upstream = sensorEven;
			}
			else {
				upstream = sensorOdd;
			}
			System.out.println(ReactiveStateUtils.scan(upstream)
			                                     .toString());
			upstream.onNext(data);
		}

		sensorEven.onComplete();
		sensorEven = null;
		sensorOdd.onComplete();
		sensorOdd = null;

	}

	private SensorData computeMin(SensorData sd1, SensorData sd2) {
		return (null != sd2 ? (sd2.getValue() < sd1.getValue() ? sd2 : sd1) : sd1);
	}

	public class SensorData implements Comparable<SensorData> {

		private final Long  id;
		private final Float value;

		public SensorData(Long id, Float value) {
			this.id = id;
			this.value = value;
		}

		public Long getId() {
			return id;
		}

		public Float getValue() {
			return value;
		}

		@Override
		public int compareTo(SensorData other) {
			if (null == other) {
				return 1;
			}
			return value.compareTo(other.getValue());
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof SensorData)) {
				return false;
			}
			SensorData other = (SensorData) obj;
			return (Long.compare(other.getId(), id) == 0) && (Float.compare(other.getValue(), value) == 0);
		}

		@Override
		public int hashCode() {
			return id != null ? id.hashCode() : 0;
		}

		@Override
		public String toString() {
			return "SensorData{" +
					"id=" + id +
					", value=" + value +
					'}';
		}
	}

	TestSubscriber<Long>   ts;
	ReplayProcessor<Long> emitter1;
	ReplayProcessor<Long> emitter2;

	@Before
	public void anotherBefore() {
		ts = new TestSubscriber<Long>();
		emitter1 = ReplayProcessor.create();
		emitter2 = ReplayProcessor.create();
		emitter1.connect();
		emitter2.connect();
	}

	private void emitValues() {
		emitter1.onNext(1L);
		emitter2.onNext(2L);
		emitter1.onNext(3L);
		emitter2.onNext(4L);
		emitter1.onComplete();
		emitter2.onComplete();
	}

	@Test
	public void mergeWithInterleave() {
		Flux.merge(emitter1, emitter2).subscribe(ts);
		emitValues();
		ts.assertValues(1L, 2L, 3L, 4L).assertComplete();
	}

	@Test
	public void mergeWithNoInterleave() throws Exception{
		Flux.concat(emitter1.log("test1"), emitter2.log("test2")).log().subscribe(ts);
		emitValues();
		ts.assertValues(1L, 3L, 2L, 4L).assertComplete();
	}



	@Test
	public void sampleCombineLatestTest() throws Exception {
		int elements = 40;
		CountDownLatch latch = new CountDownLatch(elements / 2 - 2);

		Cancellation tail = Flux.combineLatest(
				sensorOdd().cache().delay(Duration.ofMillis(100)),
				sensorEven().cache().delay(Duration.ofMillis(200)),
				this::computeMin)
		                        .log("combineLatest")
		                        .subscribe(i -> latch.countDown(), null, latch::countDown);

		generateData(elements);

		awaitLatch(null, latch);
	}

}
