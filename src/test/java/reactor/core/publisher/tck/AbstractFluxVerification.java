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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.SchedulerGroup;
import reactor.core.subscriber.SignalEmitter;
import reactor.core.scheduler.Timer;
import reactor.core.publisher.Flux;

/**
 * @author Stephane Maldini
 */
public abstract class AbstractFluxVerification extends org.reactivestreams.tck.IdentityProcessorVerification<Integer> {


	private final Map<Thread, AtomicLong> counters = new ConcurrentHashMap<>();

	protected final int batch = 1024;

	public AbstractFluxVerification() {
		super(new TestEnvironment(1000, true));
	}

	final ExecutorService executorService = Executors.newCachedThreadPool();

	final Queue<Processor<Integer, Integer>> processorReferences = new ConcurrentLinkedQueue<>();

	@Override
	public ExecutorService publisherExecutorService() {
		return executorService;
	}

	@BeforeClass
	@Before
	public void setup() {
		Timer.global();
	}

	@AfterClass
	@After
	public void tearDown() {
		executorService.shutdown();
	}

	@Override
	public Integer createElement(int element) {
		return  element;
	}

	@Override
	public Processor<Integer, Integer> createIdentityProcessor(int bufferSize) {
		counters.clear();
		final Processor<Integer, Integer> p = createProcessor(bufferSize);

		/*Streams.interval(200, TimeUnit.MILLISECONDS)
		  .consume(i -> System.out.println(p.debug()) );*/

		processorReferences.add(p);
		return p;
	}

	@Override
	public void required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError()
			throws Throwable {
		throw new SkipException("Skipped due to asynchronous buffer drained before exception");
	}

	@Override
	public Publisher<Integer> createFailedPublisher() {
		return Flux.error(new Exception("oops")).cast(Integer.class);
	}

	public abstract Processor<Integer, Integer> createProcessor(int bufferSize);

	protected void monitorThreadUse(Object val) {
		AtomicLong counter = counters.get(Thread.currentThread());
		if (counter == null) {
			counter = new AtomicLong();
			counters.put(Thread.currentThread(), counter);
		}
		counter.incrementAndGet();
	}

	@Override
	public Publisher<Integer> createHelperPublisher(long elements) {
		if (elements < 100 && elements > 0) {
			List<Integer> list = new ArrayList<Integer>();
			for (int i = 1; i <= elements; i++) {
				list.add(i);
			}

			return Flux
			  .fromIterable(list)
			  .log("iterable-publisher")
			  .filter(integer -> true)
			  .map(integer -> integer);

		} else {
			final Random random = new Random();

			return Flux
			  .<Integer>generate((n, s) -> s.onNext(random.nextInt()))
			  .log("random-publisher")
			  .map(Math::abs);
		}
	}


	/*@Test
	public void testAlotOfHotStreams() throws InterruptedException{
		for(int i = 0; i<10000; i++)
			testHotIdentityProcessor();
	}*/

	@Test
	public void testColdIdentityProcessor() throws InterruptedException {
		counters.clear();
		final int elements = 10;
		CountDownLatch latch = new CountDownLatch(elements + 1);

		Processor<Integer, Integer> processor = createProcessor(16);

		createHelperPublisher(10).subscribe(processor);

		if(Flux.class.isAssignableFrom(processor.getClass())) {
			System.out.println(((Flux)processor).debug());
		}
		List<Integer> list = new ArrayList<>();

		processor.subscribe(new Subscriber<Integer>() {
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
				System.out.println(counters);
				latch.countDown();
			}
		});
		//stream.broadcastComplete();

		latch.await(8, TimeUnit.SECONDS);
		if(Flux.class.isAssignableFrom(processor.getClass())) {
			System.out.println(((Flux)processor).debug());
		}

		long count = latch.getCount();
		org.junit.Assert.assertTrue("Count > 0 : " + count + " (" + list + ")  , Running on " +
				SchedulerGroup.DEFAULT_POOL_SIZE + " CPU", latch.getCount() == 0);

	}

	/*@Test
	public void test100Hot() throws InterruptedException {
		for (int i = 0; i < 10000; i++) {
			testHotIdentityProcessor();
		}
	}
*/
	@Test
	public void testHotIdentityProcessor() throws InterruptedException {
		counters.clear();
		final int elements = 10000;
		CountDownLatch latch = new CountDownLatch(elements);

		Processor<Integer, Integer> processor = createProcessor(1024);

		EmitterProcessor<Integer> stream = EmitterProcessor.create();
		SignalEmitter<Integer> session = SignalEmitter.create(stream);
		stream.subscribe(processor);
		if(Flux.class.isAssignableFrom(processor.getClass())) {
			System.out.println(((Flux)processor).debug());
		}

		processor.subscribe(new Subscriber<Integer>() {
			@Override
			public void onSubscribe(Subscription s) {
				s.request(elements);
			}

			@Override
			public void onNext(Integer integer) {
				latch.countDown();
			}

			@Override
			public void onError(Throwable t) {
				System.out.println("error! " + t);
			}

			@Override
			public void onComplete() {
				System.out.println("completed!");
				//latch.countDown();
			}
		});


		System.out.println(stream.debug());
		for (int i = 0; i < elements; i++) {
			if(session.submit(i, 1000) == -1){
				System.out.println(stream.debug());
			}
		}
		//stream.then();

		latch.await(8, TimeUnit.SECONDS);
		System.out.println(stream.debug());

		System.out.println(counters);
		long count = latch.getCount();
		org.junit.Assert.assertTrue("Count > 0 : " + count + "  , Running on " +
				SchedulerGroup.DEFAULT_POOL_SIZE + " CPU", latch.getCount() == 0);


		stream.onComplete();

	}


	static {
		System.setProperty("reactor.trace.cancel", "true");
	}
}
