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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.fail;

public class SchedulersTest {

	final static class TestSchedulers implements Schedulers.Factory {

		final Scheduler      elastic  = Schedulers.Factory.super.newElastic(60, Thread::new);
		final Scheduler      single   = Schedulers.Factory.super.newSingle(Thread::new);
		final Scheduler      parallel =	Schedulers.Factory.super.newParallel(1, Thread::new);

		TestSchedulers(boolean disposeOnInit) {
			if (disposeOnInit) {
				elastic.dispose();
				single.dispose();
				parallel.dispose();
			}
		}

		public final Scheduler newElastic(int ttlSeconds, ThreadFactory threadFactory) {
			assertThat(((Schedulers.SchedulerThreadFactory)threadFactory).get()).isEqualTo("unused");
			return elastic;
		}

		public final Scheduler newParallel(int parallelism, ThreadFactory threadFactory) {
			assertThat(((Schedulers.SchedulerThreadFactory)threadFactory).get()).isEqualTo("unused");
			return parallel;
		}

		public final Scheduler newSingle(ThreadFactory threadFactory) {
			assertThat(((Schedulers.SchedulerThreadFactory)threadFactory).get()).isEqualTo("unused");
			return single;
		}
	}

	@After
	public void resetSchedulers() {
		Schedulers.resetFactory();
	}

	@Test
	public void testOverride() throws InterruptedException {

		TestSchedulers ts = new TestSchedulers(true);
		Schedulers.setFactory(ts);

		Assert.assertEquals(ts.single, Schedulers.newSingle("unused"));
		Assert.assertEquals(ts.elastic, Schedulers.newElastic("unused"));
		Assert.assertEquals(ts.parallel, Schedulers.newParallel("unused"));

		Schedulers.resetFactory();

		Scheduler s = Schedulers.newSingle("unused");
		s.dispose();

		Assert.assertNotSame(ts.single, s);
	}

	@Test
	public void testShutdownOldOnSetFactory() {
		Schedulers.Factory ts1 = new Schedulers.Factory() { };
		Schedulers.Factory ts2 = new TestSchedulers(false);
		Schedulers.setFactory(ts1);
		Scheduler cachedTimerOld = Schedulers.single();
		Scheduler standaloneTimer = Schedulers.newSingle("standaloneTimer");


		Assert.assertNotSame(cachedTimerOld, standaloneTimer);
		Assert.assertNotSame(cachedTimerOld.schedule(() -> {}), Scheduler.REJECTED);
		Assert.assertNotSame(standaloneTimer.schedule(() -> {}), Scheduler.REJECTED);

		Schedulers.setFactory(ts2);
		Scheduler cachedTimerNew = Schedulers.newSingle("unused");

		Assert.assertEquals(cachedTimerNew, Schedulers.newSingle("unused"));
		Assert.assertNotSame(cachedTimerNew, cachedTimerOld);
		//assert that the old factory"s cached scheduler was shut down
		Disposable disposable = cachedTimerOld.schedule(() -> {});
		System.out.println(disposable);
		Assert.assertEquals(disposable, Scheduler.REJECTED);
		//independently created schedulers are still the programmer"s responsibility
		Assert.assertNotSame(standaloneTimer.schedule(() -> {}), Scheduler.REJECTED);
		//new factory = new alive cached scheduler
		Assert.assertNotSame(cachedTimerNew.schedule(() -> {}), Scheduler.REJECTED);
	}

	@Test
	public void testUncaughtHookCalledWhenOnErrorNotImplemented() {
		AtomicBoolean handled = new AtomicBoolean(false);
		Schedulers.onHandleError((t, e) -> handled.set(true));

		try {
			Schedulers.handleError(Exceptions.errorCallbackNotImplemented(new IllegalArgumentException()));
		} finally {
			Schedulers.resetOnHandleError();
		}
		Assert.assertTrue("errorCallbackNotImplemented not handled", handled.get());
	}

	@Test
	public void testUncaughtHookCalledWhenCommonException() {
		AtomicBoolean handled = new AtomicBoolean(false);
		Schedulers.onHandleError((t, e) -> handled.set(true));

		try {
			Schedulers.handleError(new IllegalArgumentException());
		} finally {
			Schedulers.resetOnHandleError();
		}
		Assert.assertTrue("IllegalArgumentException not handled", handled.get());
	}

	@Test
	public void testUncaughtHookNotCalledWhenThreadDeath() {
		AtomicBoolean handled = new AtomicBoolean(false);
		AtomicReference<String> failure = new AtomicReference<>(null);
		Thread.setDefaultUncaughtExceptionHandler((t, e) -> failure.set("unexpected call to default" +
				" UncaughtExceptionHandler from " + t.getName() + ": " + e));
		Schedulers.onHandleError((t, e) -> {
			handled.set(true);
			failure.set("Fatal JVM error was unexpectedly handled in " + t.getName() + ": " + e);
		});
		ThreadDeath fatal = new ThreadDeath();

		try {
			Schedulers.handleError(fatal);
			fail("expected fatal ThreadDeath exception");
		}
		catch (ThreadDeath e) {
			Assert.assertSame(e, fatal);
		}
		finally {
			Schedulers.resetOnHandleError();
		}
		Assert.assertFalse("threadDeath not silenced", handled.get());
		if (failure.get() != null) {
			fail(failure.get());
		}
	}

	@Test
	public void testRejectingSingleScheduler() {
		assertRejectingScheduler(Schedulers.newSingle("test"));
	}

	@Test
	public void testRejectingParallelScheduler() {
		assertRejectingScheduler(Schedulers.newParallel("test"));
	}

	@Test
	public void testRejectingExecutorServiceScheduler() {
		assertRejectingScheduler(Schedulers.fromExecutorService(Executors.newSingleThreadExecutor()));
	}

	public void assertRejectingScheduler(Scheduler scheduler) {
		try {
			DirectProcessor<String> p = DirectProcessor.create();

			AtomicReference<String> r = new AtomicReference<>();
			CountDownLatch l = new CountDownLatch(1);

			p.publishOn(scheduler)
			 .log()
			 .subscribe(r::set, null, l::countDown);

			scheduler.dispose();

			p.onNext("reject me");
			l.await(3, TimeUnit.SECONDS);
		}
		catch (Exception ree) {
			ree.printStackTrace();
			Throwable throwable = Exceptions.unwrap(ree);
			if (throwable instanceof RejectedExecutionException) {
				return;
			}
			fail(throwable + " is not a RejectedExecutionException");
		}
		finally {
			scheduler.dispose();
		}
	}

	//private final int             BUFFER_SIZE     = 8;
	private final AtomicReference<Throwable> exceptionThrown = new AtomicReference<>();
	private final int                        N               = 17;

	@Test
	public void testDispatch() throws Exception {
		Scheduler service = Schedulers.newSingle(r -> {
			Thread t = new Thread(r, "dispatcher");
			t.setUncaughtExceptionHandler((t1, e) -> exceptionThrown.set(e));
			return t;
		});

		service.dispose();
	}

	Scheduler.Worker runTest(final Scheduler.Worker dispatcher)
			throws InterruptedException {
		CountDownLatch tasksCountDown = new CountDownLatch(N);

		dispatcher.schedule(() -> {
			for (int i = 0; i < N; i++) {
				dispatcher.schedule(tasksCountDown::countDown);
			}
		});

		boolean check = tasksCountDown.await(10, TimeUnit.SECONDS);
		if (exceptionThrown.get() != null) {
			exceptionThrown.get()
			               .printStackTrace();
		}
		Assert.assertTrue(exceptionThrown.get() == null);
		Assert.assertTrue(check);

		return dispatcher;
	}

	@Test
	public void immediateTaskIsExecuted() throws Exception {
		Scheduler serviceRB = Schedulers.newSingle("rbWork");
		Scheduler.Worker r = serviceRB.createWorker();

		long start = System.currentTimeMillis();
		AtomicInteger latch = new AtomicInteger(1);
		Consumer<String> c =  ev -> {
			latch.decrementAndGet();
			try {
				System.out.println("ev: "+ev);
				Thread.sleep(1000);
			}
			catch(InterruptedException ie){
				throw Exceptions.propagate(ie);
			}
		};
		r.schedule(() -> c.accept("Hello World!"));

		Thread.sleep(1200);
		long end = System.currentTimeMillis();

		serviceRB.dispose();

		Assert.assertTrue("Event missed", latch.intValue() == 0);
		Assert.assertTrue("Timeout too long", (end - start) >= 1000);
	}

	@Test
	public void immediateTaskIsSkippedIfDisposeRightAfter() throws Exception {
		Scheduler serviceRB = Schedulers.newSingle("rbWork");
		Scheduler.Worker r = serviceRB.createWorker();

		long start = System.currentTimeMillis();
		AtomicInteger latch = new AtomicInteger(1);
		Consumer<String> c =  ev -> {
			latch.decrementAndGet();
			try {
				System.out.println("ev: "+ev);
				Thread.sleep(1000);
			}
			catch(InterruptedException ie){
				throw Exceptions.propagate(ie);
			}
		};
		r.schedule(() -> c.accept("Hello World!"));
		serviceRB.dispose();

		Thread.sleep(1200);
		long end = System.currentTimeMillis();


		Assert.assertTrue("Task not skipped", latch.intValue() == 1);
		Assert.assertTrue("Timeout too long", (end - start) >= 1000);
	}

	@Test
	public void singleSchedulerPipelining() throws Exception {
		Scheduler serviceRB = Schedulers.newSingle("rb", true);
		Scheduler.Worker dispatcher = serviceRB.createWorker();

		try {
			Thread t1 = Thread.currentThread();
			Thread[] t2 = { null };

			CountDownLatch cdl = new CountDownLatch(1);

			dispatcher.schedule(() -> { t2[0] = Thread.currentThread(); cdl.countDown(); });

			if (!cdl.await(5, TimeUnit.SECONDS)) {
				Assert.fail("single timed out");
			}

			Assert.assertNotSame(t1, t2[0]);
		} finally {
			dispatcher.dispose();
		}
	}

	@Test
	public void testCachedSchedulerDelegates() {
		Scheduler mock = new Scheduler() {
			@Override
			public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
				throw new IllegalStateException("scheduleTaskDelay");
			}

			@Override
			public Disposable schedulePeriodically(Runnable task, long initialDelay,
					long period, TimeUnit unit) {
				throw new IllegalStateException("schedulePeriodically");
			}

			@Override
			public Worker createWorker() {
				throw new IllegalStateException("createWorker");
			}

			@Override
			public Disposable schedule(Runnable task) {
				throw new IllegalStateException("scheduleTask");
			}

			@Override
			public boolean isDisposed() {
				throw new IllegalStateException("isDisposed");
			}

			@Override
			public void dispose() {
				throw new IllegalStateException("dispose");
			}

			@Override
			public long now(TimeUnit unit) {
				throw new IllegalStateException("now");
			}

			@Override
			public void start() {
				throw new IllegalStateException("start");
			}

		};

		Schedulers.CachedScheduler cached = new Schedulers.CachedScheduler("cached", mock);

		//dispose is bypassed by the cached version
		cached.dispose();
		cached.dispose();

		//other methods delegate
		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> cached.schedule(null))
	            .withMessage("scheduleTask");

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> cached.schedule(null, 1000, TimeUnit.MILLISECONDS))
	            .withMessage("scheduleTaskDelay");

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> cached.schedulePeriodically(null, 1000, 1000, TimeUnit.MILLISECONDS))
	            .withMessage("schedulePeriodically");

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> cached.now(TimeUnit.MILLISECONDS))
	            .withMessage("now");

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(cached::start)
	            .withMessage("start");

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(cached::createWorker)
	            .withMessage("createWorker");

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(cached::isDisposed)
	            .withMessage("isDisposed");
	}


	@Test(timeout = 5000)
	public void parallelSchedulerThreadCheck() throws Exception{
		Scheduler s = Schedulers.newParallel("work", 2);
		try {
			Scheduler.Worker w = s.createWorker();

			Thread currentThread = Thread.currentThread();
			AtomicReference<Thread> taskThread = new AtomicReference<>(currentThread);
			CountDownLatch latch = new CountDownLatch(1);

			w.schedule(() -> {
				taskThread.set(Thread.currentThread());
				latch.countDown();
			});

			latch.await();

			assertThat(taskThread.get()).isNotEqualTo(currentThread);
		}
		finally {
			s.dispose();
		}
	}

	@Test(timeout = 5000)
	public void singleSchedulerThreadCheck() throws Exception{
		Scheduler s = Schedulers.newSingle("work");
		try {
			Scheduler.Worker w = s.createWorker();

			Thread currentThread = Thread.currentThread();
			AtomicReference<Thread> taskThread = new AtomicReference<>(currentThread);
			CountDownLatch latch = new CountDownLatch(1);

			w.schedule(() -> {
				taskThread.set(Thread.currentThread());
				latch.countDown();
			});

			latch.await();

			assertThat(taskThread.get()).isNotEqualTo(currentThread);
		}
		finally {
			s.dispose();
		}
	}


	@Test(timeout = 5000)
	public void elasticSchedulerThreadCheck() throws Exception{
		Scheduler s = Schedulers.newElastic("work");
		try {
			Scheduler.Worker w = s.createWorker();

			Thread currentThread = Thread.currentThread();
			AtomicReference<Thread> taskThread = new AtomicReference<>(currentThread);
			CountDownLatch latch = new CountDownLatch(1);

			w.schedule(() -> {
				taskThread.set(Thread.currentThread());
				latch.countDown();
			});

			latch.await();

			assertThat(taskThread.get()).isNotEqualTo(currentThread);
		}
		finally {
			s.dispose();
		}
	}

	@Test(timeout = 5000)
	public void executorThreadCheck() throws Exception{
		ExecutorService es = Executors.newSingleThreadExecutor();
		Scheduler s = Schedulers.fromExecutor(es::execute);

		try {
			Scheduler.Worker w = s.createWorker();

			Thread currentThread = Thread.currentThread();
			AtomicReference<Thread> taskThread = new AtomicReference<>(currentThread);
			CountDownLatch latch = new CountDownLatch(1);

			w.schedule(() -> {
				taskThread.set(Thread.currentThread());
				latch.countDown();
			});

			latch.await();

			assertThat(taskThread.get()).isNotEqualTo(currentThread);
		}
		finally {
			s.dispose();
			es.shutdownNow();
		}
	}

	@Test(timeout = 5000)
	public void executorThreadCheck2() throws Exception{
		ExecutorService es = Executors.newSingleThreadExecutor();
		Scheduler s = Schedulers.fromExecutor(es::execute, true);

		try {
			Scheduler.Worker w = s.createWorker();

			Thread currentThread = Thread.currentThread();
			AtomicReference<Thread> taskThread = new AtomicReference<>(currentThread);
			CountDownLatch latch = new CountDownLatch(1);

			w.schedule(() -> {
				taskThread.set(Thread.currentThread());
				latch.countDown();
			});

			latch.await();

			assertThat(taskThread.get()).isNotEqualTo(currentThread);
		}
		finally {
			s.dispose();
			es.shutdownNow();
		}
	}

	@Test(timeout = 5000)
	public void sharedSingleCheck() throws Exception{
		Scheduler p = Schedulers.newParallel("shared");
		Scheduler s = Schedulers.single(p);

		try {
			for(int i = 0; i < 3; i++) {
				Scheduler.Worker w = s.createWorker();

				Thread currentThread = Thread.currentThread();
				AtomicReference<Thread> taskThread = new AtomicReference<>(currentThread);
				CountDownLatch latch = new CountDownLatch(1);

				w.schedule(() -> {
					taskThread.set(Thread.currentThread());
					latch.countDown();
				});

				latch.await();

				assertThat(taskThread.get()).isNotEqualTo(currentThread);
			}
		}
		finally {
			s.dispose();
			p.dispose();
		}
	}

	void recursiveCall(Scheduler.Worker w, CountDownLatch latch, int data){
		if (data < 2) {
			latch.countDown();
			w.schedule(() -> recursiveCall(w,  latch,data + 1));
		}
	}

	@Test
	public void recursiveParallelCall() throws Exception {
		Scheduler s = Schedulers.newParallel("work", 4);
		try {
			Scheduler.Worker w = s.createWorker();

			CountDownLatch latch = new CountDownLatch(2);

			w.schedule(() -> recursiveCall(w, latch, 0));

			latch.await();
		}
		finally {
			s.dispose();
		}
	}

	@Test
	public void pingPongParallelCall() throws Exception {
		Scheduler s = Schedulers.newParallel("work", 4);
		try {
			Scheduler.Worker w = s.createWorker();
			Thread t = Thread.currentThread();
			AtomicReference<Thread> t1 = new AtomicReference<>(t);
			AtomicReference<Thread> t2 = new AtomicReference<>(t);

			CountDownLatch latch = new CountDownLatch(4);

			AtomicReference<Runnable> pong = new AtomicReference<>();

			Runnable ping = () -> {
				if(latch.getCount() > 0){
					t1.set(Thread.currentThread());
					w.schedule(pong.get());
					latch.countDown();
				}
			};

			pong.set(() -> {
				if(latch.getCount() > 0){
					t2.set(Thread.currentThread());
					w.schedule(ping);
					latch.countDown();
				}
			});

			w.schedule(ping);

			latch.await();

			assertThat(t).isNotEqualTo(t1.get());
			assertThat(t).isNotEqualTo(t2.get());
		}
		finally {
			s.dispose();
		}
	}

	@Test
	public void restartParallel() {
		restart(Schedulers.newParallel("test"));
	}

//	@Test
//	public void restartTimer() {
//		restart(Schedulers.newTimer("test"));
//	}
//
//	@Test
//	public void restartElastic() {
//		restart(Schedulers.newElastic("test"));
//	}

	@Test
	public void restartSingle(){
		restart(Schedulers.newSingle("test"));
	}

	void restart(Scheduler s){
		Thread t = Mono.fromCallable(Thread::currentThread)
		               .subscribeOn(s)
		               .block();

		s.dispose();
		s.start();

		Thread t2 = Mono.fromCallable(Thread::currentThread)
		                .subscribeOn(s)
		                .block();

		assertThat(t).isNotEqualTo(Thread.currentThread());
		assertThat(t).isNotEqualTo(t2);
	}

	@Test
	public void testDefaultMethods(){
		EmptyScheduler s = new EmptyScheduler();

		s.dispose();
		assertThat(s.disposeCalled).isTrue();

		EmptyScheduler.EmptyWorker w = s.createWorker();
		w.dispose();
		assertThat(w.disposeCalled).isTrue();


		EmptyTimedScheduler ts = new EmptyTimedScheduler();
		ts.dispose();//noop
		ts.start();
		EmptyTimedScheduler.EmptyTimedWorker tw = ts.createWorker();
		tw.dispose();

		long before = System.currentTimeMillis();

		assertThat(ts.now(TimeUnit.MILLISECONDS)).isGreaterThanOrEqualTo(before)
		                                         .isLessThanOrEqualTo(System.currentTimeMillis());

//		assertThat(tw.now(TimeUnit.MILLISECONDS)).isGreaterThanOrEqualTo(before)
//		                                        .isLessThanOrEqualTo(System.currentTimeMillis());

		//noop
		new Schedulers(){

		};

		//noop
		Schedulers.elastic().dispose();
	}

	final static class EmptyScheduler implements Scheduler {

		boolean disposeCalled;

		@Override
		public void dispose() {
			disposeCalled = true;
		}

		@Override
		public Disposable schedule(Runnable task) {
			return null;
		}

		@Override
		public EmptyWorker createWorker() {
			return new EmptyWorker();
		}

		static class EmptyWorker implements Worker {

			boolean disposeCalled;

			@Override
			public Disposable schedule(Runnable task) {
				return null;
			}

			@Override
			public void dispose() {
				disposeCalled = true;
			}
		}
	}

	final static class EmptyTimedScheduler implements Scheduler {

		@Override
		public Disposable schedule(Runnable task) {
			return null;
		}

		@Override
		public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			return null;
		}

//		@Override
//		public Disposable schedulePeriodically(Runnable task,
//				long initialDelay,
//				long period,
//				TimeUnit unit) {
//			return null;
//		}

		@Override
		public EmptyTimedWorker createWorker() {
			return new EmptyTimedWorker();
		}

		static class EmptyTimedWorker implements Worker {

			@Override
			public Disposable schedule(Runnable task) {
				return null;
			}

			@Override
			public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
				return null;
			}

			@Override
			public Disposable schedulePeriodically(Runnable task,
					long initialDelay,
					long period,
					TimeUnit unit) {
				return null;
			}

			@Override
			public void dispose() {
			}
		}
	}
}
