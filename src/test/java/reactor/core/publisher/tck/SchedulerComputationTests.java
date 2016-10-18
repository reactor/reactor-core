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
import java.util.function.Consumer;

import org.junit.Assert;
import org.reactivestreams.Processor;
import org.testng.SkipException;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.FluxProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.core.Exceptions;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class SchedulerComputationTests extends AbstractProcessorVerification {

	@Override
	public Processor<Long, Long> createProcessor(int bufferSize) {
		Scheduler scheduler = Schedulers.newParallel("shared-work", 2);
		EmitterProcessor<Long> e = EmitterProcessor.create();
		return FluxProcessor.wrap(e,
				e.publishOn(scheduler)
				 .doAfterTerminate(scheduler::shutdown).awaitOnSubscribe());
	}

	@Override
	public long maxSupportedSubscribers() {
		return 1L;
	}

	//@Test
	public void simpleTestC() throws Exception {
		//for(int i = 0; i < 1000; i++){
//			System.out.println("new test "+i);
			simpleTest();
		//}
	}

	@Override
	public void simpleTest() throws Exception {
		Scheduler serviceRB = Schedulers.newSingle("rbWork");
		Scheduler.Worker r = serviceRB.createWorker();

		long start = System.currentTimeMillis();
		CountDownLatch latch = new CountDownLatch(1);
		Consumer<String> c =  ev -> {
				latch.countDown();
			try {
				System.out.println("ev: "+ev);
				Thread.sleep(1000);
			}
			catch(InterruptedException ie){
				throw Exceptions.propagate(ie);
			}
		};
		r.schedule(() -> c.accept("Hello World!"));

		serviceRB.shutdown();
		Thread.sleep(1200);
		long end = System.currentTimeMillis();

		Assert.assertTrue("Event missed", latch.getCount() == 0);
		Assert.assertTrue("Timeout too long", (end - start) >= 1000);

	}

	@Override
	public void required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError() throws
	  Throwable {
		throw new SkipException("Optional requirement");
	}

}
