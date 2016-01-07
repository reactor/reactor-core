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

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.Flux;
import reactor.Mono;
import reactor.Processors;
import reactor.Subscribers;
import reactor.core.processor.FluxProcessor;
import reactor.core.subscription.ReactiveSession;
import reactor.core.support.Assert;
import reactor.core.support.Logger;
import reactor.core.support.ReactiveStateUtils;
import reactor.fn.Consumer;

/**
 * @author Stephane Maldini
 */
public class CombinationTests {

	private static final Logger LOG = Logger.getLogger(CombinationTests.class);

	private Processor<SensorData, SensorData> sensorEven;
	private Processor<SensorData, SensorData> sensorOdd;

	@Before
	public void before() {
		sensorEven();
		sensorOdd();
	}

	@After
	public void after() {
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

		Stream<Integer> stream = Publishers.merge(Publishers.map(Publishers.just(1), i -> Streams.range(0, n)));

		final CountDownLatch latch = new CountDownLatch(n);
		awaitLatch(stream.consume(integer -> latch.countDown()), latch);
	}*/

	@Test
	public void tesSubmitSession() throws Exception {
		FluxProcessor<Integer, Integer> processor = Processors.emitter();
		AtomicInteger count = new AtomicInteger();
		processor.to(Processors.ioGroup()
		                       .get())
		         .subscribe(Subscribers.create(s -> {
			         try {
				         Thread.sleep(1000);
			         }
			         catch (InterruptedException ie) {
				         //IGNORE
			         }
			         s.request(1L);
			         return null;
		         }, (d, s) -> count.incrementAndGet()));

		ReactiveSession<Integer> session = processor.startSession();
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

		Assert.isTrue(count.get() == 1, "latch : " + count);
		Assert.isTrue(emission >= 1, "time : " + emission);
	}

	@Test
	public void testEmitter() throws Throwable {
		FluxProcessor<Integer, Integer> processor = Processors.emitter();

		int n = 100_000;
		int subs = 4;
		final CountDownLatch latch = new CountDownLatch((n + 1) * subs);

		for (int i = 0; i < subs; i++) {
			processor.to(Processors.singleGroup()
			                       .get())
			         .subscribe(Subscribers.create(s -> {
				         s.request(1L);
				         return null;
			         }, (d, s) -> {
//				         System.out.println(d);
				         s.request(1L);
				         latch.countDown();
			         }, null, d -> latch.countDown()));
		}

		ReactiveSession<Integer> session = processor.startSession();

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
		Assert.isTrue(waited, "latch : " + latch.getCount());
	}

	public Publisher<SensorData> sensorOdd() {
		if (sensorOdd == null) {
			// this is the stream we publish odd-numbered events to
			this.sensorOdd = Processors.blackbox(Processors.topic("odd"), p -> p.log("odd"));

			// add substream to "master" list
			//allSensors().add(sensorOdd.reduce(this::computeMin).timeout(1000));
		}

		return sensorOdd;
	}

	public Publisher<SensorData> sensorEven() {
		if (sensorEven == null) {
			// this is the stream we publish even-numbered events to
			this.sensorEven = Processors.blackbox(Processors.topic("even"), p -> p.log("even"));

			// add substream to "master" list
			//allSensors().add(sensorEven.reduce(this::computeMin).timeout(1000));
		}
		return sensorEven;
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
		Processor<SensorData, SensorData> sensorDataProcessor = Processors.<SensorData>singleGroup().get();

		sensorDataProcessor.subscribe(Subscribers.unbounded((d, sub) -> latch.countDown(),
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
}
