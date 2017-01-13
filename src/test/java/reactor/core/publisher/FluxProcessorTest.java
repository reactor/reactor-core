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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class FluxProcessorTest {


	@Test
	public void testSubmitSession() throws Exception {
		FluxProcessor<Integer, Integer> processor = EmitterProcessor.create();
		AtomicInteger count = new AtomicInteger();
		CountDownLatch latch = new CountDownLatch(1);
		Scheduler scheduler = Schedulers.parallel();
		processor.publishOn(scheduler)
		         .delaySubscriptionMillis(1000)
		         .subscribe(d -> {
			         count.incrementAndGet();
			         latch.countDown();
		         }, 1);

		BlockingSink<Integer> session = processor.connectSink();
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

		latch.await(5, TimeUnit.SECONDS);
		Assert.assertTrue("latch : " + count, count.get() == 1);
		Assert.assertTrue("time : " + emission, emission >= 0);
		scheduler.dispose();
	}

	@Test
	public void testEmitter() throws Throwable {
		FluxProcessor<Integer, Integer> processor = EmitterProcessor.create();

		int n = 100_000;
		int subs = 4;
		final CountDownLatch latch = new CountDownLatch((n + 1) * subs);
		Scheduler c = Schedulers.single();
		for (int i = 0; i < subs; i++) {
			processor.publishOn(c)
			         .limitRate(1)
			         .subscribe(d -> latch.countDown(), null, latch::countDown);
		}

		BlockingSink<Integer> session = processor.connectSink();

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
		Assert.assertTrue( "latch : " + latch.getCount(), waited);
		c.dispose();
	}
	@Test
	public void testEmitter2() throws Throwable {
		FluxProcessor<Integer, Integer> processor = EmitterProcessor.create();

		int n = 100_000;
		int subs = 4;
		final CountDownLatch latch = new CountDownLatch((n + 1) * subs);
		Scheduler c = Schedulers.single();
		for (int i = 0; i < subs; i++) {
			processor.publishOn(c)
			         .doOnComplete(latch::countDown)
			         .doOnNext(d -> latch.countDown())
			         .subscribe(Integer.MAX_VALUE);
		}

		BlockingSink<Integer> session = processor.connectSink();

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
		Assert.assertTrue( "latch : " + latch.getCount(), waited);
		c.dispose();
	}

}