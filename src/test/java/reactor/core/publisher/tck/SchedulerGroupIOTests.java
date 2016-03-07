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
import java.util.function.Consumer;

import org.reactivestreams.Processor;
import org.testng.SkipException;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.SchedulerGroup;
import reactor.core.util.Assert;
import reactor.core.util.Exceptions;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class SchedulerGroupIOTests extends AbstractProcessorVerification {

	@Override
	public Processor<Long, Long> createProcessor(int bufferSize) {
		return FluxProcessor.async(SchedulerGroup.io("shared-work", bufferSize, 2, Throwable::printStackTrace, () ->
		{}));
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
		SchedulerGroup serviceRB = SchedulerGroup.async("rbWork", 32, 1);
		Consumer<Runnable>  r = serviceRB.call();

		long start = System.currentTimeMillis();
		CountDownLatch latch = new CountDownLatch(1);
		Consumer<String> c =  ev -> {
				latch.countDown();
			try {
				System.out.println("ev: "+ev);
				Thread.sleep(1000);
			}
			catch(InterruptedException ie){
				throw Exceptions.fail(ie);
			}
		};
		r.accept(() -> c.accept("Hello World!"));

		boolean success = serviceRB.awaitAndShutdown(3, TimeUnit.SECONDS);
		long end = System.currentTimeMillis();

		Assert.isTrue(latch.getCount() == 0, "Event missed");
		Assert.isTrue(success, "Shutdown failed");
		Assert.isTrue((end - start) >= 1000, "Timeout too long");

	}

	@Override
	public void required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError() throws
	  Throwable {
		throw new SkipException("Optional requirement");
	}

}
