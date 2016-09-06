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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class BlockingTests {

	static Scheduler exec;

	@BeforeClass
	public static void before() {
		exec = Schedulers.fromExecutorService(Executors.newSingleThreadExecutor());
	}

	@AfterClass
	public static void after() {
		exec.shutdown();
	}

	@Test
	public void blockingFirst() {
		Assert.assertEquals((Integer) 1,
				Flux.range(1, 10)
				    .publishOn(exec)
				    .blockFirst());
	}

	@Test
	public void blockingLast() {
		Assert.assertEquals((Integer) 10,
				Flux.range(1, 10)
				    .publishOn(exec)
				    .blockLast());
	}

	@Test(expected = RuntimeException.class)
	public void blockingFirstError() {
		Flux.error(new RuntimeException("test"))
		    .publishOn(exec)
		    .blockFirst();
	}

	@Test(expected = RuntimeException.class)
	public void blockingLastError() {
		Flux.error(new RuntimeException("test"))
		    .publishOn(exec)
		    .blockLast();
	}

	@Test
	public void blockingLastInterrupted() throws Exception {
		CountDownLatch latch = new CountDownLatch(1);
		Thread t = new Thread(() -> {
			try {
				Flux.never()
				    .blockLast();
			}
			catch (Exception e) {
				if (Exceptions.unwrap(e) instanceof InterruptedException) {
					latch.countDown();
				}
			}
		});

		t.start();
		Thread.sleep(1000);
		t.interrupt();

		Assert.assertTrue("Not interrupted ?", latch.await(3, TimeUnit.SECONDS));
	}
}
