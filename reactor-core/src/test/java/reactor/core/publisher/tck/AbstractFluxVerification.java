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
package reactor.core.publisher.tck;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Stephane Maldini
 */
public abstract class AbstractFluxVerification extends org.reactivestreams.tck.IdentityProcessorVerification<Integer> {


	private final Map<Thread, AtomicLong> counters = new ConcurrentHashMap<>();

	protected final int batch = 1024;

	public AbstractFluxVerification() {
		super(new TestEnvironment(true));
	}

	final ExecutorService executorService = Executors.newCachedThreadPool();

	final Queue<Processor<Integer, Integer>> processorReferences = new ConcurrentLinkedQueue<>();

	@Override
	public ExecutorService publisherExecutorService() {
		return executorService;
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
			List<Integer> list = new ArrayList<>();
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

			return Mono.fromCallable(random::nextInt)
			           .repeat()
			           .map(Math::abs);
		}
	}


	/*@Test
	public void testAlotOfHotStreams() throws InterruptedException{
		for(int i = 0; i<10000; i++)
			testHotIdentityProcessor();
	}*/

	static {
		System.setProperty("reactor.trace.cancel", "true");
	}
}
