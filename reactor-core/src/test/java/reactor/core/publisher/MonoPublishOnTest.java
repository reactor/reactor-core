/*
 * Copyright (c) 2015-2026 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.Scheduler;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
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
			CountDownLatch finallyLatch = new CountDownLatch(1);
			CountDownLatch inOnNextLatch = new CountDownLatch(1);

			Hooks.onOperatorError((t, d) -> {
				throwableInOnOperatorError.set(t);
				dataInOnOperatorError.set(d);
				return t;
			});

			ExecutorService executor = newCachedThreadPool();
			CountDownLatch latch = new CountDownLatch(1);

			AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();
			Mono.just(data)
			    .publishOn(fromExecutorService(executor))
			    .doOnNext(s -> {
				    try {
					    inOnNextLatch.countDown();
					    latch.await();
				    }
				    catch (InterruptedException e) {
				    }
			    })
			    .publishOn(fromExecutor(executor))
			    .doFinally(s -> finallyLatch.countDown())
			    .subscribe(assertSubscriber);

			inOnNextLatch.await();
			executor.shutdownNow();

			finallyLatch.await();

			assertSubscriber.assertNoValues()
			                .assertError(RejectedExecutionException.class)
			                .assertNotComplete();

			assertThat(throwableInOnOperatorError.get()).isInstanceOf(RejectedExecutionException.class);
			assertThat(data).isSameAs(dataInOnOperatorError.get());
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
			CountDownLatch finallyLatch = new CountDownLatch(1);
			CountDownLatch inOnNextLatch = new CountDownLatch(1);

			Hooks.onOperatorError((t, d) -> {
				throwableInOnOperatorError.set(t);
				dataInOnOperatorError.set(d);
				return t;
			});

			ExecutorService executor = newCachedThreadPool();
			CountDownLatch latch = new CountDownLatch(1);

			AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();
			Mono.just(data)
			    .publishOn(fromExecutorService(executor))
			    .doOnNext(s -> {
				    try {
					    inOnNextLatch.countDown();
					    latch.await();
				    }
				    catch (InterruptedException e) {
					    throw Exceptions.propagate(exception);
				    }
			    })
			    .publishOn(fromExecutor(executor))
			    .doFinally(s -> finallyLatch.countDown())
			    .subscribe(assertSubscriber);

			inOnNextLatch.await();
			executor.shutdownNow();

			finallyLatch.await();
			assertSubscriber.assertNoValues()
			                .assertError(RejectedExecutionException.class)
			                .assertNotComplete();

			assertThat(throwableInOnOperatorError.get()).isInstanceOf(RejectedExecutionException.class);
			assertThat(exception).isSameAs(throwableInOnOperatorError.get()
					.getSuppressed()[0]);
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
			CountDownLatch finallyLatch = new CountDownLatch(1);
			CountDownLatch inOnNextLatch = new CountDownLatch(1);

			Hooks.onOperatorError((t, d) -> {
				throwableInOnOperatorError.set(t);
				dataInOnOperatorError.set(d);
				return t;
			});

			ExecutorService executor = newCachedThreadPool();
			CountDownLatch latch = new CountDownLatch(1);

			AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();
			Mono.just(data)
			    .publishOn(fromExecutorService(executor))
			    .doOnNext(s -> {
				    try {
					    inOnNextLatch.countDown();
					    latch.await();
				    }
				    catch (InterruptedException e) {
				    }
			    })
			    .publishOn(fromExecutorService(executor))
			    .doFinally(s -> finallyLatch.countDown())
			    .subscribe(assertSubscriber);

			inOnNextLatch.await();

			executor.shutdownNow();

			finallyLatch.await();
			assertSubscriber.assertNoValues()
			                .assertError(RejectedExecutionException.class)
			                .assertNotComplete();

			assertThat(throwableInOnOperatorError.get()).isInstanceOf(RejectedExecutionException.class);
			assertThat(data).isSameAs(dataInOnOperatorError.get());
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
			CountDownLatch finallyLatch = new CountDownLatch(1);
			CountDownLatch inOnNextLatch = new CountDownLatch(1);

			Hooks.onOperatorError((t, d) -> {
				throwableInOnOperatorError.set(t);
				dataInOnOperatorError.set(d);
				return t;
			});

			ExecutorService executor = newCachedThreadPool();
			CountDownLatch latch = new CountDownLatch(1);

			AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();
			Mono.just(data)
			    .publishOn(fromExecutorService(executor))
			    .doOnNext(s -> {
				    try {
					    inOnNextLatch.countDown();
					    latch.await();
				    }
				    catch (InterruptedException e) {
					    throw Exceptions.propagate(exception);
				    }
			    })
			    .publishOn(fromExecutorService(executor))
			    .doFinally(s -> finallyLatch.countDown())
			    .subscribe(assertSubscriber);

			inOnNextLatch.await();

			executor.shutdownNow();

			finallyLatch.await();
			assertSubscriber.assertNoValues()
			                .assertError(RejectedExecutionException.class)
			                .assertNotComplete();

			assertThat(throwableInOnOperatorError.get()).isInstanceOf(RejectedExecutionException.class);
			assertThat(exception).isSameAs(throwableInOnOperatorError.get()
					.getSuppressed()[0]);
		}
		finally {
			Hooks.resetOnOperatorError();
		}
	}

	@Test
	@Disabled
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
				fail("Bubbling RejectedExecutionException expected");
			}
			catch (Exception e) {
				assertThat(Exceptions.unwrap(e)).isInstanceOf(RejectedExecutionException.class);
			}
		}
		finally {
			latch.countDown();
			executor.shutdownNow();
		}

		executor.shutdownNow();
	}

	@Test
	@Disabled
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
				fail("Bubbling RejectedExecutionException expected");
			}
			catch (Exception e) {
				assertThat(Exceptions.unwrap(e)).isInstanceOf(RejectedExecutionException.class);
			}
		}
		finally {
			latch.countDown();
			executor.shutdownNow();
		}
	}

	@Test
	public void scanOperator() {
		MonoPublishOn<String> test = new MonoPublishOn<>(Mono.empty(), Schedulers.immediate());

		assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.immediate());
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoPublishOn.PublishOnSubscriber<String> test = new MonoPublishOn.PublishOnSubscriber<>(
				actual, Schedulers.single());
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.single());
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanSubscriberError() {
		CoreSubscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoPublishOn.PublishOnSubscriber<String> test = new MonoPublishOn.PublishOnSubscriber<>(
				actual, Schedulers.single());

		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
	}

	@Test
	public void error() {
		StepVerifier.create(Mono.error(new RuntimeException("forced failure"))
		                        .publishOn(Schedulers.single()))
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void errorHide() {
		StepVerifier.create(Mono.error(new RuntimeException("forced failure"))
		                        .hide()
		                        .publishOn(Schedulers.single()))
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void discardsPendingValueOnCancel() throws InterruptedException {
		List<Object> discarded = new ArrayList<>();
		Scheduler scheduler = Schedulers.newSingle("monoPublishOnDiscard");
		CountDownLatch occupied = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		try {
			// occupy the only worker thread so the value cannot be emitted before the cancel
			scheduler.schedule(() -> {
				occupied.countDown();
				try {
					release.await(10, java.util.concurrent.TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			});
			assertThat(occupied.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

			TestPublisher<String> source = TestPublisher.create();
			AssertSubscriber<String> ts = AssertSubscriber.create(1);
			source.mono()
			      .publishOn(scheduler)
			      .doOnDiscard(Object.class, discarded::add)
			      .subscribe(ts);

			source.next("value");
			ts.cancel();

			assertThat(discarded).as("discarded").containsExactly("value");
		}
		finally {
			release.countDown();
			scheduler.dispose();
		}
	}

}
