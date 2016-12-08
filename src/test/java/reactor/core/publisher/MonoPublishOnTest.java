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

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import reactor.core.Exceptions;
import reactor.test.subscriber.AssertSubscriber;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static reactor.core.scheduler.Schedulers.fromExecutor;
import static reactor.core.scheduler.Schedulers.fromExecutorService;

public class MonoPublishOnTest {

	@Test
	public void normal() {
	}


	@Test
	public void rejectedExecutionExceptionOnDataSignalExecutor()
	{

		ExecutorService executor = newCachedThreadPool();

		CountDownLatch latch = new CountDownLatch(1);

		AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();
    Mono.just(1)
		    .publishOn(fromExecutor(executor))
		    .doOnNext(s -> {
		      try {
		        latch.await();
		      } catch (InterruptedException e) {
	      }})
		    .publishOn(fromExecutor(executor))
		    .subscribe(assertSubscriber);

		executor.shutdownNow();
		latch.countDown();

		assertSubscriber
				.await()
				.assertError(RejectedExecutionException.class)
				.assertNotComplete();
	}

	@Test
	public void rejectedExecutionExceptionOnErrorSignalExecutor()
	{

		ExecutorService executor = newCachedThreadPool();

		CountDownLatch latch = new CountDownLatch(1);

		AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();
		Mono.just(1)
				.publishOn(fromExecutor(executor))
				.doOnNext(s -> {
  				try {
		  				latch.await();
			  	} catch (InterruptedException e) {
				  		throw Exceptions.propagate(e);
				  }})
				.publishOn(fromExecutor(executor)).subscribe(assertSubscriber);

		executor.shutdownNow();

		assertSubscriber
				.await()
				.assertError(RejectedExecutionException.class)
				.assertNotComplete();
	}

	@Test
	public void rejectedExecutionExceptionOnDataSignalExecutorService()
	{

		ExecutorService executor = newCachedThreadPool();

		CountDownLatch latch = new CountDownLatch(1);

		AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();
		Mono.just(1)
				.publishOn(fromExecutorService(executor))
				.doOnNext(s -> {
					try {
						latch.await();
					}
					catch (InterruptedException e) {
					}})
				.publishOn(fromExecutorService(executor))
				.subscribe(assertSubscriber);

		executor.shutdownNow();

		assertSubscriber
				.await()
				.assertError(RejectedExecutionException.class)
				.assertNotComplete();
		}

		@Test
		public void rejectedExecutionExceptionOnErrorSignalExecutorService()
		{

		ExecutorService executor = newCachedThreadPool();

		CountDownLatch latch = new CountDownLatch(1);

		AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();
		Mono.just(1)
				.publishOn(fromExecutorService(executor))
				.doOnNext(s -> {
					try {
						latch.await();
					} catch (InterruptedException e) {
						throw Exceptions.propagate(e);
					}})
				.publishOn(fromExecutorService(executor)).subscribe(assertSubscriber);

		executor.shutdownNow();
		latch.countDown();

		assertSubscriber
				.await()
				.assertError(RejectedExecutionException.class)
				.assertNotComplete();
		}

}
