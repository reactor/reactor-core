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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import reactor.core.publisher.Flux;

/**
 * @author Stephane Maldini
 */
public abstract class AbstractProcessorVerification extends org.reactivestreams.tck.IdentityProcessorVerification<Long> {

	final ExecutorService executorService = Executors.newCachedThreadPool();

	final Queue<Processor<Long, Long>> processorReferences = new ConcurrentLinkedQueue<>();

	@Override
	public void required_spec208_mustBePreparedToReceiveOnNextSignalsAfterHavingCalledSubscriptionCancel()
			throws Throwable {
		throw new SkipException("Delivery guarantee with Exception#failWithCancel");
	}

	@Override
	public ExecutorService publisherExecutorService() {
		return executorService;
	}

	public AbstractProcessorVerification() {
		super(new TestEnvironment(500));
	}

	@Override
	public void required_spec309_requestNegativeNumberMustSignalIllegalArgumentException() throws Throwable {
		throw new SkipException("Need RS review");
	}

	@Override
	public void required_spec313_cancelMustMakeThePublisherEventuallyDropAllReferencesToTheSubscriber()
			throws Throwable {
		throw new SkipException("Need RS review");
	}

	@Override
	public void required_spec309_requestZeroMustSignalIllegalArgumentException() throws Throwable {
		throw new SkipException("Need RS review");
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
		return Flux.log(super.createHelperPublisher(elements), "publisher");
	}*/

	/*@Override
	public Publisher<Long> createHelperPublisher(long elements) {
		return Flux.<Long, AtomicLong>create(
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
