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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Processor;
import org.testng.SkipException;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.FluxProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class SchedulerParallelTests extends AbstractProcessorVerification {

	//private final int             BUFFER_SIZE     = 8;
	private final AtomicReference<Throwable> exceptionThrown = new AtomicReference<>();
	private final int             N               = 17;

	@Override
	public Processor<Long, Long> createProcessor(int bufferSize) {
		EmitterProcessor<Long> e = EmitterProcessor.create();
		Scheduler s = Schedulers.newSingle("shared-async");
		return FluxProcessor.wrap(e, e.publishOn(s).doOnComplete(s::shutdown).awaitOnSubscribe());

	}

	@Override
	public void required_createPublisher3MustProduceAStreamOfExactly3Elements() throws Throwable {
		super.required_createPublisher3MustProduceAStreamOfExactly3Elements();
	}

	@Override
	public void required_spec313_cancelMustMakeThePublisherEventuallyDropAllReferencesToTheSubscriber()
			throws Throwable {
		super.required_spec313_cancelMustMakeThePublisherEventuallyDropAllReferencesToTheSubscriber();
	}

	@Override
	public void required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError() throws
	  Throwable {
		throw new SkipException("Optional requirement");
	}
/*
	@Override
	public void required_mustRequestFromUpstreamForElementsThatHaveBeenRequestedLongAgo() throws Throwable {
		throw new SkipException("Optional multi subscribe requirement");
	}*/

	@Override
	public long maxSupportedSubscribers() {
		return 1L;
	}

	@Test
	public void testDispatch() throws Exception {
		Scheduler service = Schedulers.newSingle(r -> {
			Thread t = new Thread(r, "dispatcher");
			t.setUncaughtExceptionHandler((t1, e) -> exceptionThrown.set(e));
			return t;
		});

		service.shutdown();
	}

	Scheduler.Worker runTest(final Scheduler.Worker dispatcher) throws InterruptedException {
		CountDownLatch tasksCountDown = new CountDownLatch(N);

		dispatcher.schedule(() -> {
			for (int i = 0; i < N; i++) {
				dispatcher.schedule(tasksCountDown::countDown);
			}
		});

		boolean check = tasksCountDown.await(10, TimeUnit.SECONDS);
		if(exceptionThrown.get()!= null){
			exceptionThrown.get().printStackTrace();
		}
		Assert.assertTrue(exceptionThrown.get() == null);
		Assert.assertTrue(check);

		return dispatcher;
	}

}