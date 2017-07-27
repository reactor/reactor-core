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

package reactor.core.scheduler;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.test.StepVerifier;

public class RejectedExecutionTest {

	private BoundedScheduler scheduler;

	private ConcurrentLinkedQueue<Long> onNexts = new ConcurrentLinkedQueue<>();
	private ConcurrentLinkedQueue<Throwable> onErrors = new ConcurrentLinkedQueue<>();
	private ConcurrentLinkedQueue<Object> onNextDropped = new ConcurrentLinkedQueue<>();
	private ConcurrentLinkedQueue<Throwable> onErrorDropped = new ConcurrentLinkedQueue<>();
	private ConcurrentLinkedQueue<Throwable> onOperatorError = new ConcurrentLinkedQueue<>();

	@Before
	public void setUp() {
		scheduler = new BoundedScheduler(3, Schedulers.newSingle("bounded-single"));
		Hooks.onNextDropped(o -> onNextDropped.add(o));
		Hooks.onErrorDropped(e -> onErrorDropped.add(e));
		Hooks.onOperatorError((e, o) -> {
			onOperatorError.add(e);
			return e;
		});
	}

	@After
	public void tearDown() {
		scheduler.dispose();
	}

	/**
	 * Test: onNext cannot be delivered due to RejectedExecutionExceptiob
	 * Current behaviour:
	 *   No onNext, onError, onNextDropped, onErrorDropped generated
	 *   Exception:
	 *   [parallel-1] ERROR reactor.core.scheduler.Schedulers - Scheduler worker in group main failed with an uncaught exception
	 *		java.util.concurrent.RejectedExecutionException: null
	 *			at reactor.core.scheduler.RejectedExecutionTest$BoundedScheduler$BoundedWorker.schedule(RejectedExecutionTest.java:228) ~[bin/:na]
	 *			at reactor.core.publisher.FluxPublishOn$PublishOnSubscriber.trySchedule(FluxPublishOn.java:294) ~[bin/:na]
	 *			at reactor.core.publisher.FluxPublishOn$PublishOnSubscriber.onNext(FluxPublishOn.java:234) ~[bin/:na]
	 *			at reactor.core.publisher.FluxTake$TakeSubscriber.onNext(FluxTake.java:118) ~[bin/:na]
	 *			at reactor.core.publisher.FluxInterval$IntervalRunnable.run(FluxInterval.java:105) ~[bin/:na]
	 *			at reactor.core.scheduler.ParallelScheduler$ParallelWorker$ParallelWorkerTask.run(ParallelScheduler.java:367) ~[bin/:na]
	 *			at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511) [na:1.8.0_77]
	 *			at java.util.concurrent.FutureTask.runAndReset(FutureTask.java:308) [na:1.8.0_77]
	 *			at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$301(ScheduledThreadPoolExecutor.java:180) [na:1.8.0_77]
	 *			at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:294) [na:1.8.0_77]
	 *			at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142) [na:1.8.0_77]
	 *			at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617) [na:1.8.0_77]
	 *			at java.lang.Thread.run(Thread.java:745) [na:1.8.0_77]
	 *
	 */
	@Test
	public void publishOn() throws Exception {
		Flux<Long> flux = Flux.interval(Duration.ofMillis(2)).take(255)
				.publishOn(scheduler)
				.doOnNext(i -> onNext(i))
				.doOnError(e -> onError(e));

		verifyRejectedExecutionConsistency(flux, 5);
	}


	/**
	 * Test: onNext cannot be delivered due to RejectedExecutionExceptiob
	 * Current behaviour:
	 *   No onNext, onError, onNextDropped, onErrorDropped generated
	 *   Sequence of exceptions for each flatMap element:
	 *
	 *   [parallel-1] ERROR reactor.core.scheduler.Schedulers - Scheduler worker in group main failed with an uncaught exception
	 *		java.util.concurrent.RejectedExecutionException: null
	 *			at reactor.core.scheduler.RejectedExecutionTest$BoundedScheduler.schedule(RejectedExecutionTest.java:208) ~[bin/:na]
	 *			at reactor.core.publisher.FluxSubscribeOnValue$ScheduledScalar.request(FluxSubscribeOnValue.java:125) ~[bin/:na]
	 *			at reactor.core.publisher.FluxPeek$PeekSubscriber.request(FluxPeek.java:131) ~[bin/:na]
	 *			at reactor.core.publisher.FluxPeek$PeekSubscriber.request(FluxPeek.java:131) ~[bin/:na]
	 *			at reactor.core.publisher.FluxFlatMap$FlatMapInner.onSubscribe(FluxFlatMap.java:901) ~[bin/:na]
	 *			at reactor.core.publisher.FluxPeek$PeekSubscriber.onSubscribe(FluxPeek.java:164) ~[bin/:na]
	 *			at reactor.core.publisher.FluxPeek$PeekSubscriber.onSubscribe(FluxPeek.java:164) ~[bin/:na]
	 *			at reactor.core.publisher.MonoSubscribeOnValue.subscribe(MonoSubscribeOnValue.java:61) ~[bin/:na]
	 *			at reactor.core.publisher.MonoPeek.subscribe(MonoPeek.java:71) ~[bin/:na]
	 *			at reactor.core.publisher.MonoPeek.subscribe(MonoPeek.java:71) ~[bin/:na]
	 *			at reactor.core.publisher.Mono.subscribe(Mono.java:2859) ~[bin/:na]
	 *			at reactor.core.publisher.FluxFlatMap$FlatMapMain.onNext(FluxFlatMap.java:376) ~[bin/:na]
	 *			at reactor.core.publisher.FluxTake$TakeSubscriber.onNext(FluxTake.java:118) ~[bin/:na]
	 *			at reactor.core.publisher.FluxInterval$IntervalRunnable.run(FluxInterval.java:105) ~[bin/:na]
	 *			at reactor.core.scheduler.ParallelScheduler$ParallelWorker$ParallelWorkerTask.run(ParallelScheduler.java:367) ~[bin/:na]
	 *			at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511) [na:1.8.0_77]
	 *			at java.util.concurrent.FutureTask.runAndReset(FutureTask.java:308) [na:1.8.0_77]
	 *			at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$301(ScheduledThreadPoolExecutor.java:180) [na:1.8.0_77]
	 *			at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:294) [na:1.8.0_77]
	 *			at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142) [na:1.8.0_77]
	 *			at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617) [na:1.8.0_77]
	 *			at java.lang.Thread.run(Thread.java:745) [na:1.8.0_77]
	 *
	 */
	@Test
	public void publishOnFlatMap() throws Exception {
		Flux<Long> flux = Flux.interval(Duration.ofMillis(2)).take(255)
				.flatMap(j -> Mono.just(j)
								.publishOn(scheduler)
								.doOnNext(i -> onNext(i))
								.doOnError(e -> onError(e)));

		verifyRejectedExecutionConsistency(flux, 5);
	}

	/**
	 * Test: onNext cannot be delivered due to RejectedExecutionExceptiob
	 * Current behaviour:
	 *   No onNext, onError, onNextDropped, onErrorDropped generated
	 *   Exception:
	 * 		[parallel-1] ERROR reactor.core.scheduler.Schedulers - Scheduler worker in group main failed with an uncaught exception
	 *		java.util.concurrent.RejectedExecutionException: null
	 *			at reactor.core.scheduler.RejectedExecutionTest$BoundedScheduler$BoundedWorker.schedule(RejectedExecutionTest.java:283) ~[bin/:na]
	 *			at reactor.core.publisher.FluxPublishOn$PublishOnSubscriber.trySchedule(FluxPublishOn.java:294) ~[bin/:na]
	 *			at reactor.core.publisher.FluxPublishOn$PublishOnSubscriber.onNext(FluxPublishOn.java:234) ~[bin/:na]
	 *			at reactor.core.publisher.ParallelSource$ParallelSourceMain.drainAsync(ParallelSource.java:333) ~[bin/:na]
	 *			at reactor.core.publisher.ParallelSource$ParallelSourceMain.drain(ParallelSource.java:473) ~[bin/:na]
	 *			at reactor.core.publisher.ParallelSource$ParallelSourceMain.onNext(ParallelSource.java:233) ~[bin/:na]
	 *			at reactor.core.publisher.FluxTake$TakeSubscriber.onNext(FluxTake.java:118) ~[bin/:na]
	 *			at reactor.core.publisher.FluxInterval$IntervalRunnable.run(FluxInterval.java:105) ~[bin/:na]
	 *			at reactor.core.scheduler.ParallelScheduler$ParallelWorker$ParallelWorkerTask.run(ParallelScheduler.java:367) ~[bin/:na]
	 *			at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511) [na:1.8.0_77]
	 *			at java.util.concurrent.FutureTask.runAndReset(FutureTask.java:308) [na:1.8.0_77]
	 *			at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$301(ScheduledThreadPoolExecutor.java:180) [na:1.8.0_77]
	 *			at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:294) [na:1.8.0_77]
	 *			at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142) [na:1.8.0_77]
	 *			at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617) [na:1.8.0_77]
	 *			at java.lang.Thread.run(Thread.java:745) [na:1.8.0_77]
	 */
	@Test
	public void parallelRunOn() throws Exception {
		ParallelFlux<Long> flux = Flux.interval(Duration.ofMillis(2)).take(255)
				.parallel(1)
				.runOn(scheduler)
				.doOnNext(i -> onNext(i))
				.doOnError(e -> onError(e));

		verifyRejectedExecutionConsistency(flux, 5);
	}

	/**
	 * Test: Subscription fails because `subscribeOn` scheduler rejects execution
	 * Current behaviour: `subscribe` throws RejectedExecutionException
	 *
	 * FIXME: onNext/onError are on a different scheduler. Not sure how to get them to be scheduled on
	 * the `subscriberOn` scheduler.
	 *
	 * Exception:
	 * java.util.concurrent.RejectedExecutionException
	 *			at reactor.core.scheduler.RejectedExecutionTest$BoundedScheduler$BoundedWorker.schedule(RejectedExecutionTest.java:330)
	 *			at reactor.core.publisher.FluxSubscribeOn.subscribe(FluxSubscribeOn.java:63)
	 *			at reactor.core.publisher.Flux.subscribe(Flux.java:6376)
	 *			at reactor.core.publisher.Flux.subscribeWith(Flux.java:6444)
	 *			at reactor.core.publisher.Flux.subscribe(Flux.java:6369)
	 *			at reactor.core.publisher.Flux.subscribe(Flux.java:6333)
	 *			at reactor.core.publisher.Flux.subscribe(Flux.java:6251)
	 *			at reactor.core.scheduler.RejectedExecutionTest.subscribeOn(RejectedExecutionTest.java:195)
	 */
	@Test
	public void subscribeOn() throws Exception {
		scheduler.tasksRemaining.set(0);
		Flux<Long> flux = Flux.interval(Duration.ofMillis(2)).take(255)
				.doOnNext(i -> onNexts.add(i))
				.doOnError(e -> onErrors.add(e))
				.doOnSubscribe(s -> System.out.println("onSubscribe on thread " + Thread.currentThread().getName()))
				.doOnRequest(n -> System.out.println("onRequest on thread " + Thread.currentThread().getName() + " " + n))
				.subscribeOn(scheduler);

		try {
			flux.subscribe();
		} catch (RejectedExecutionException e) {
			e.printStackTrace();
			// Expected exception
		}
	}

	/**
	 * Test: Subscription of the flatMap entries are rejected
	 * Current behaviour: Error not propagated.
	 * Exception:
	 *	[parallel-1] ERROR reactor.core.scheduler.Schedulers - Scheduler worker in group main failed with an uncaught exception
	 *	java.util.concurrent.RejectedExecutionException: null
	 *			at reactor.core.scheduler.RejectedExecutionTest$BoundedScheduler$BoundedWorker.schedule(RejectedExecutionTest.java:312) ~[bin/:na]
	 *			at reactor.core.publisher.MonoSubscribeOn$SubscribeOnSubscriber.request(MonoSubscribeOn.java:150) ~[bin/:na]
	 *			at reactor.core.publisher.FluxFlatMap$FlatMapInner.onSubscribe(FluxFlatMap.java:901) ~[bin/:na]
	 *			at reactor.core.publisher.MonoSubscribeOn.subscribe(MonoSubscribeOn.java:50) ~[bin/:na]
	 *			at reactor.core.publisher.Mono.subscribe(Mono.java:2859) ~[bin/:na]
	 *			at reactor.core.publisher.FluxFlatMap$FlatMapMain.onNext(FluxFlatMap.java:376) ~[bin/:na]
	 *			at reactor.core.publisher.FluxTake$TakeSubscriber.onNext(FluxTake.java:118) ~[bin/:na]
	 *			at reactor.core.publisher.FluxInterval$IntervalRunnable.run(FluxInterval.java:105) ~[bin/:na]
	 *			at reactor.core.scheduler.ParallelScheduler$ParallelWorker$ParallelWorkerTask.run(ParallelScheduler.java:367) ~[bin/:na]
	 *			at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511) [na:1.8.0_77]
	 *			at java.util.concurrent.FutureTask.runAndReset(FutureTask.java:308) [na:1.8.0_77]
	 *			at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$301(ScheduledThreadPoolExecutor.java:180) [na:1.8.0_77]
	 *			at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:294) [na:1.8.0_77]
	 *			at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142) [na:1.8.0_77]
	 *			at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617) [na:1.8.0_77]
	 *			at java.lang.Thread.run(Thread.java:745) [na:1.8.0_77]
	 */
	@Test
	public void flatMapSubscribeOn() throws Exception {
		Flux<Long> flux = Flux.interval(Duration.ofMillis(2)).take(255)
				.flatMap(j -> Mono.just(j)
								.doOnNext(i -> onNexts.add(i))
								.doOnError(e -> onErrors.add(e))
								.subscribeOn(scheduler));

		verifyRejectedExecutionConsistency(flux, 5);
	}


	private void verifyRejectedExecutionConsistency(Publisher<Long> flux, int maxTasks) {

		StepVerifier verifier = StepVerifier.create(flux, 0)
					.expectSubscription()
					.then(() -> scheduler.tasksRemaining.set(maxTasks))
					.thenRequest(255)
					.expectNextCount(255)
					.expectComplete(); // FIXME: Should this be RejectedExecutionException error?

		try {
			verifier.verify(Duration.ofSeconds(5));
		} catch (Throwable t) {
			// FIXME: At the moment, the tests time out: no onNext or onError generated
			t.printStackTrace();
			assertTrue("Unexpected exception: " + t, t.getMessage().contains("VerifySubscriber timed out"));
		}
		assertTrue("Too many onNexts " + onNexts.size(), onNexts.size() < 255);
		assertEquals(0, onErrors.size()); // FIXME: RejectedExecutionException not propagated
		assertEquals(0, onNextDropped.size()); // FIXME: onNext dropped silently
		assertEquals(0, onErrorDropped.size()); // FIXME: onError not generated
	}

	private void onNext(long i) {
		String thread = Thread.currentThread().getName();
		assertTrue("onNext on the wrong thread " + thread, thread.contains("bounded"));
		onNexts.add(i);
	}

	private void onError(Throwable t) {
		String thread = Thread.currentThread().getName();
		assertTrue("onError on the wrong thread " + thread, thread.contains("bounded"));
		onErrors.add(t);
	}

	private class BoundedScheduler implements Scheduler {

		AtomicInteger tasksRemaining = new AtomicInteger(Integer.MAX_VALUE);

		final Scheduler actual;

		BoundedScheduler(int bound, Scheduler actual) {
			this.actual = actual;
		}

		@Override
		public void dispose() {
			actual.dispose();
		}

		@Override
		public Disposable schedule(Runnable task) {
			if (tasksRemaining.decrementAndGet() < 0)
				throw new RejectedExecutionException();
			return actual.schedule(task);
		}

		@Override
		public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			if (tasksRemaining.decrementAndGet() < 0)
				throw new RejectedExecutionException();
			return actual.schedule(task, delay, unit);
		}

		@Override
		public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
			return actual.schedulePeriodically(task, initialDelay, period, unit);
		}

		@Override
		public Worker createWorker() {
			return new BoundedWorker(actual.createWorker());
		}

		@Override
		public boolean isDisposed() {
			return actual.isDisposed();
		}

		private class BoundedWorker implements Worker {

			final Worker actual;

			BoundedWorker(Worker actual) {
				this.actual = actual;
			}

			@Override
			public void dispose() {
				actual.dispose();
			}

			@Override
			public Disposable schedule(Runnable task) {
				if (tasksRemaining.decrementAndGet() < 0)
					throw new RejectedExecutionException();
				return actual.schedule(task);
			}
		}
	}
}
