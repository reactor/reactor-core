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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import reactor.Flux;
import reactor.Timers;

/**
 * @author Stephane Maldini
 */
public abstract class AbstractProcessorVerification extends org.reactivestreams.tck.IdentityProcessorVerification<Long> {

	final ExecutorService executorService = Executors.newCachedThreadPool();

	final Queue<Processor<Long, Long>> processorReferences = new ConcurrentLinkedQueue<>();

	@Test
	public void simpleTest() throws Exception{

	}

	@Override
	public ExecutorService publisherExecutorService() {
		return executorService;
	}

	public AbstractProcessorVerification() {
		super(new TestEnvironment(500));
	}

	@BeforeClass
	public void setup() {
		Timers.global();
	}

	@AfterClass
	public void tearDown() throws InterruptedException {
		executorService.submit(() -> {
			  Processor<Long, Long> p;
			  while ((p = processorReferences.poll()) != null) {
				  System.out.println("aborting");
				  p.onComplete();
			  }
		  }
		);

		executorService.shutdown();
	}

	@Override
	public Long createElement(int element) {
		return (long) element;
	}



	@Override
	public Processor<Long, Long> createIdentityProcessor(int bufferSize) {
		Processor<Long, Long> p = createProcessor(bufferSize);
		processorReferences.add(p);
		return p;
	}

	protected abstract Processor<Long, Long> createProcessor(int bufferSize);

	/*@Override
	public Publisher<Long> createHelperPublisher(long elements) {
		return Publishers.log(super.createHelperPublisher(elements), "publisher");
	}*/

	/*@Override
	public Publisher<Long> createHelperPublisher(long elements) {
		return Publishers.<Long, AtomicLong>create(
		  (s) -> {
			  long cursor = s.context().getAndIncrement();
			  if (cursor < elements) {
				  s.onNext(cursor);
			  } else {
				  s.onComplete();
			  }
		  },
		  s -> new AtomicLong(0L)
		);
	}*/

	@Override
	public Publisher<Long> createFailedPublisher() {
		return Flux.error(new Exception("test"));
	}
}
