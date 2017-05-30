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

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.subscriber.AssertSubscriber;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static reactor.core.scheduler.Schedulers.fromExecutor;
import static reactor.core.scheduler.Schedulers.fromExecutorService;

public class MonoPublishOnTest {

	@Test
	public void rejectedExecutionExceptionOnDataSignalExecutor()
			throws InterruptedException {

		int data = 1;

		final AtomicReference<Throwable> throwableInOnOperatorError =
				new AtomicReference<>();
		final AtomicReference<Object> dataInOnOperatorError = new AtomicReference<>();

		try {

			CountDownLatch hookLatch = new CountDownLatch(1);

			Hooks.onOperatorError((t, d) -> {
				throwableInOnOperatorError.set(t);
				dataInOnOperatorError.set(d);
				hookLatch.countDown();
				return t;
			});

			ExecutorService executor = newCachedThreadPool();
			CountDownLatch latch = new CountDownLatch(1);

			AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();
			Mono.just(data)
			    .publishOn(fromExecutorService(executor))
			    .doOnNext(s -> {
				    try {
					    latch.await();
				    }
				    catch (InterruptedException e) {
				    }
			    })
			    .publishOn(fromExecutor(executor))
			    .subscribe(assertSubscriber);

			executor.shutdownNow();

			assertSubscriber.assertNoValues()
			                .assertNoError()
			                .assertNotComplete();

			hookLatch.await();

			assertThat(throwableInOnOperatorError.get(),
					instanceOf(RejectedExecutionException.class));
			Assert.assertSame(dataInOnOperatorError.get(), data);
		}
		finally {
			Hooks.resetOnOperatorError();
		}
	}

	@Test
	public void rejectedExecutionExceptionOnErrorSignalExecutor()
			throws InterruptedException {

		int data = 1;
		Exception exception = new IllegalStateException();

		final AtomicReference<Throwable> throwableInOnOperatorError =
				new AtomicReference<>();
		final AtomicReference<Object> dataInOnOperatorError = new AtomicReference<>();

		try {

			CountDownLatch hookLatch = new CountDownLatch(2);

			Hooks.onOperatorError((t, d) -> {
				throwableInOnOperatorError.set(t);
				dataInOnOperatorError.set(d);
				hookLatch.countDown();
				return t;
			});

			ExecutorService executor = newCachedThreadPool();
			CountDownLatch latch = new CountDownLatch(1);

			AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();
			Mono.just(data)
			    .publishOn(fromExecutorService(executor))
			    .doOnNext(s -> {
				    try {
					    latch.await();
				    }
				    catch (InterruptedException e) {
					    throw Exceptions.propagate(exception);
				    }
			    })
			    .publishOn(fromExecutor(executor))
			    .subscribe(assertSubscriber);

			executor.shutdownNow();

			assertSubscriber.assertNoValues()
			                .assertNoError()
			                .assertNotComplete();

			hookLatch.await();

			assertThat(throwableInOnOperatorError.get(),
					instanceOf(RejectedExecutionException.class));
			Assert.assertSame(throwableInOnOperatorError.get()
			                                            .getSuppressed()[0], exception);
		}
		finally {
			Hooks.resetOnOperatorError();
		}
	}

	@Test
	public void rejectedExecutionExceptionOnDataSignalExecutorService()
			throws InterruptedException {

		int data = 1;

		final AtomicReference<Throwable> throwableInOnOperatorError =
				new AtomicReference<>();
		final AtomicReference<Object> dataInOnOperatorError = new AtomicReference<>();

		try {

			CountDownLatch hookLatch = new CountDownLatch(1);

			Hooks.onOperatorError((t, d) -> {
				throwableInOnOperatorError.set(t);
				dataInOnOperatorError.set(d);
				hookLatch.countDown();
				return t;
			});

			ExecutorService executor = newCachedThreadPool();
			CountDownLatch latch = new CountDownLatch(1);

			AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();
			Mono.just(data)
			    .publishOn(fromExecutorService(executor))
			    .doOnNext(s -> {
				    try {
					    latch.await();
				    }
				    catch (InterruptedException e) {
				    }
			    })
			    .publishOn(fromExecutorService(executor))
			    .subscribe(assertSubscriber);

			executor.shutdownNow();

			assertSubscriber.assertNoValues()
			                .assertNoError()
			                .assertNotComplete();

			hookLatch.await();

			assertThat(throwableInOnOperatorError.get(),
					instanceOf(RejectedExecutionException.class));
			Assert.assertSame(dataInOnOperatorError.get(), data);
		}
		finally {
			Hooks.resetOnOperatorError();
		}
	}

	@Test
	public void rejectedExecutionExceptionOnErrorSignalExecutorService()
			throws InterruptedException {

		int data = 1;
		Exception exception = new IllegalStateException();

		final AtomicReference<Throwable> throwableInOnOperatorError =
				new AtomicReference<>();
		final AtomicReference<Object> dataInOnOperatorError = new AtomicReference<>();

		try {

			CountDownLatch hookLatch = new CountDownLatch(2);

			Hooks.onOperatorError((t, d) -> {
				throwableInOnOperatorError.set(t);
				dataInOnOperatorError.set(d);
				hookLatch.countDown();
				return t;
			});

			ExecutorService executor = newCachedThreadPool();
			CountDownLatch latch = new CountDownLatch(1);

			AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();
			Mono.just(data)
			    .publishOn(fromExecutorService(executor))
			    .doOnNext(s -> {
				    try {
					    latch.await();
				    }
				    catch (InterruptedException e) {
					    throw Exceptions.propagate(exception);
				    }
			    })
			    .publishOn(fromExecutorService(executor))
			    .subscribe(assertSubscriber);

			executor.shutdownNow();

			assertSubscriber.assertNoValues()
			                .assertNoError()
			                .assertNotComplete();

			hookLatch.await();

			assertThat(throwableInOnOperatorError.get(),
					instanceOf(RejectedExecutionException.class));
			Assert.assertSame(throwableInOnOperatorError.get()
			                                            .getSuppressed()[0], exception);
		}
		finally {
			Hooks.resetOnOperatorError();
		}
	}

	@Test
	@Ignore
	//FIXME the behavior is not failing fast anymore, find original issue and re-evaluate
	public void rejectedExecutionSubscribeExecutorScheduler() {
		CountDownLatch latch = new CountDownLatch(1);
		ExecutorService executor = new ThreadPoolExecutor(1,
				1,
				0L,
				MILLISECONDS,
				new SynchronousQueue<>(),
				new AbortPolicy());

		try {
			executor.submit(() -> {
				try {
					latch.await();
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			});

			try {
				Mono.just(1)
				    .publishOn(fromExecutor(executor))
				    .block();
				Assert.fail("Bubbling RejectedExecutionException expected");
			}
			catch (Exception e) {
				assertThat(Exceptions.unwrap(e), instanceOf(RejectedExecutionException.class));
			}
		}
		finally {
			latch.countDown();
			executor.shutdownNow();
		}

		executor.shutdownNow();
	}

	@Test
	@Ignore
	//FIXME the behavior is not failing fast anymore, find original issue and re-evaluate
	public void rejectedExecutionSubscribeExecutorServiceScheduler() {
		CountDownLatch latch = new CountDownLatch(1);
		ExecutorService executor = new ThreadPoolExecutor(1,
				1,
				0L,
				MILLISECONDS,
				new SynchronousQueue<>(),
				new AbortPolicy());

		try {
			executor.submit(() -> {
				try {
					latch.await();
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			});

			try {
				Mono.just(1)
				    .publishOn(fromExecutor(executor))
				    .block();
				Assert.fail("Bubbling RejectedExecutionException expected");
			}
			catch (Exception e) {
				assertThat(Exceptions.unwrap(e), instanceOf(RejectedExecutionException.class));
			}
		}
		finally {
			latch.countDown();
			executor.shutdownNow();
		}
	}

	@Test
	public void scanSubscriber() {
		Subscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoPublishOn.PublishOnSubscriber<String> test = new MonoPublishOn.PublishOnSubscriber<>(
				actual, Schedulers.single());
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}

	@Test
	public void scanSubscriberError() {
		Subscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoPublishOn.PublishOnSubscriber<String> test = new MonoPublishOn.PublishOnSubscriber<>(
				actual, Schedulers.single());

		Assertions.assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isNull();
		test.onError(new IllegalStateException("boom"));
		Assertions.assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");
	}

}
