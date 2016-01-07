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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.reactivestreams.Processor;
import org.testng.SkipException;
import reactor.Processors;
import reactor.core.error.ReactorFatalException;
import reactor.core.support.Assert;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class ProcessorGroupWorkTests extends AbstractProcessorVerification {

	@Override
	public Processor<Long, Long> createProcessor(int bufferSize) {
		return Processors.<Long>ioGroup("shared-work", bufferSize, 2, Throwable::printStackTrace).get();
	}

	@Override
	public long maxSupportedSubscribers() {
		return 1L;
	}

	//@Test
	public void simpleTestC() throws Exception {
		for(int i = 0; i < 1000; i++){
			System.out.println("new test "+i);
			simpleTest();
		}
	}
	@Override
	public void simpleTest() throws Exception {
		ProcessorGroup<String> serviceRB = Processors.asyncGroup("rbWork", 32);
		BiConsumer<String, Consumer<? super String>>  r = serviceRB.dataDispatcher();

		long start = System.currentTimeMillis();
		CountDownLatch latch = new CountDownLatch(1);
		Consumer<String> c =  ev -> {
				latch.countDown();
			try {
				System.out.println("ev: "+ev);
				Thread.sleep(1000);
			}
			catch(InterruptedException ie){
				throw ReactorFatalException.create(ie);
			}
		};
		r.accept("Hello World!", c);

		boolean success = serviceRB.awaitAndShutdown(1, TimeUnit.SECONDS);
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
