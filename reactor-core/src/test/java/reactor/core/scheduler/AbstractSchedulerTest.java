/*
 * Copyright (c) 2017-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.scheduler;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.test.AutoDisposingExtension;
import reactor.test.ParameterizedTestWithName;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Stephane Maldini
 */
public abstract class AbstractSchedulerTest {

	@RegisterExtension
	public AutoDisposingExtension afterTest = new AutoDisposingExtension();

	/**
	 * @return the {@link Scheduler} to be tested, {@link Scheduler#init() initialized}
	 */
	protected abstract Scheduler scheduler();

	/**
	 * @return the {@link Scheduler} to be tested, not yet {@link Scheduler#init()
	 * initialized}
	 */
	protected abstract Scheduler freshScheduler();

	protected boolean shouldCheckInit() {
		return true;
	}

	protected boolean shouldCheckInterrupted(){
		return false;
	}

	protected boolean shouldCheckDisposeTask(){
		return true;
	}

	protected boolean shouldCheckMassWorkerDispose(){
		return true;
	}

	protected boolean shouldCheckDirectTimeScheduling() { return true; }

	protected boolean shouldCheckWorkerTimeScheduling() { return true; }

	protected boolean shouldCheckSupportRestart() { return true; }

	protected boolean shouldCheckMultipleDisposeGracefully() {
		return false;
	}

	protected Scheduler schedulerNotCached() {
		Scheduler s = scheduler();
		assertThat(s).as("common scheduler tests should not use a CachedScheduler")
				.isNotInstanceOf(Schedulers.CachedScheduler.class);
		return s;
	}

	/**
	 * Tests that return {@code true} from {@link #shouldCheckMultipleDisposeGracefully}
	 * should override this method.
	 * @param s the {@link Scheduler}
	 * @return whether the {@link Scheduler} is fully terminated
	 */
	protected boolean isTerminated(Scheduler s) {
		return s.isDisposed();
	}

	@Test
	@Disabled("Should be enabled in 3.5.0")
	void nonInitializedIsNotDisposed() {
		Scheduler s = freshScheduler();
		assertThat(s.isDisposed()).isFalse();
	}

	@Test
	void canInitializeMultipleTimesNonDisposed() {
		Scheduler s = scheduler();
		assertThatNoException().isThrownBy(s::init);
	}

	@ParameterizedTestWithName
	@ValueSource(booleans = {true, false})
	void cannotInitAfterDispose(boolean withForceDispose) {
		Assumptions.assumeThat(shouldCheckInit())
		           .as("scheduler supports restart prevention").isTrue();

		Scheduler s = scheduler();

		if (withForceDispose) {
			s.dispose();
		} else {
			s.disposeGracefully().block(Duration.ofSeconds(1));
		}

		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(s::init);
	}

	@Test
	public void restartSupport() {
		boolean supportsRestart = shouldCheckSupportRestart();
		Scheduler s = scheduler();
		s.dispose();
		// TODO: in 3.6.x: remove restart capability and this validation
		s.start();

		if (supportsRestart) {
			assertThat(s.isDisposed()).as("restart supported").isFalse();
		}
		else {
			assertThat(s.isDisposed()).as("restart not supported").isTrue();
		}
	}

	@Test
	public void acceptTaskAfterStartStopStart() {
		Assumptions.assumeThat(shouldCheckSupportRestart()).as("scheduler supports restart").isTrue();

		Scheduler scheduler = scheduler();
		scheduler.dispose();

		// TODO: in 3.6.x: remove restart capability and this validation
		scheduler.start();
		assertThatCode(() -> scheduler.schedule(() -> {})).doesNotThrowAnyException();
	}

	@Test
	@Timeout(10)
	final public void directScheduleAndDispose() throws Exception {
		Scheduler s = schedulerNotCached();

		try {
			assertThat(s.isDisposed()).isFalse();
			CountDownLatch latch = new CountDownLatch(1);
			CountDownLatch latch2 = shouldCheckDisposeTask() ? new CountDownLatch(1)
					: null;

			try {
				Disposable d = s.schedule(() -> {
					try {
						latch.countDown();
						if (latch2 != null && !latch2.await(10,
								TimeUnit.SECONDS) && shouldCheckInterrupted()) {
							fail("Should have interrupted");
						}
					}
					catch (InterruptedException e) {
					}
				});

				latch.await();
				if(shouldCheckDisposeTask()) {
					assertThat(d.isDisposed()).isFalse();
				}
				else{
					d.isDisposed(); //noop
				}
				d.dispose();
				d.dispose();//noop
			} catch (Throwable schedulingError) {
				fail("unexpected scheduling error", schedulingError);
			}

			Thread.yield();

			if(latch2 != null) {
				latch2.countDown();
			}

			s.dispose();
			s.dispose();//noop

			if(s == ImmediateScheduler.instance()){
				return;
			}
			assertThat(s.isDisposed()).isTrue();

			try {
				Disposable d = s.schedule(() -> {
					if(shouldCheckInterrupted()){
						try {
							Thread.sleep(10000);
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
				});

				d.dispose();
				assertThat(d.isDisposed()).isTrue();
			} catch (Throwable schedulingError) {
				assertThat(schedulingError).isInstanceOf(RejectedExecutionException.class);
			}
		}
		finally {
			s.dispose();
			s.dispose();//noop
		}
	}

	@ParameterizedTestWithName
	@ValueSource(booleans = {true, false})
	void concurrentDisposeGracefully(boolean withForceDispose) throws Exception {
		if (!shouldCheckMultipleDisposeGracefully()) {
			return;
		}

		Scheduler s = scheduler();
		ExecutorService executor = Executors.newFixedThreadPool(10);

		CountDownLatch finishShutdownLatch = new CountDownLatch(1);
		s.schedule(() -> {
			while (true) {
				try {
					// ignore cancellation, hold graceful shutdown
					if (finishShutdownLatch.await(100, TimeUnit.MILLISECONDS)) {
						return;
					}
				} catch (InterruptedException ignored) {
				}
			}
		});

		CountDownLatch tasksLatch = new CountDownLatch(10);

		BlockingQueue<Exception> exceptions = new ArrayBlockingQueue<>(10);
		for (int i = 0; i < 10; i++) {
			executor.submit(() -> {
				try {
					if (withForceDispose) {
						s.disposeGracefully().timeout(Duration.ofMillis(40)).retry(5).onErrorResume(e -> Mono.fromRunnable(s::dispose));
					} else {
						s.disposeGracefully().timeout(Duration.ofMillis(40)).block();
					}
				} catch (Exception e) {
					exceptions.add(e);
				}
				tasksLatch.countDown();
			});
		}

		tasksLatch.await(100, TimeUnit.MILLISECONDS);

		if (withForceDispose) {
			assertThat(exceptions).isEmpty();
		} else {
			assertThat(exceptions).hasSize(10).allMatch(e -> e.getCause() instanceof TimeoutException);
		}
		assertThatException()
				.isThrownBy(() -> s.disposeGracefully().timeout(Duration.ofMillis(40)).block())
				.withCauseExactlyInstanceOf(TimeoutException.class);

		finishShutdownLatch.countDown();

		assertThatNoException().isThrownBy(
				() -> s.disposeGracefully().timeout(Duration.ofMillis(100)).block()
		);
		assertThatNoException().isThrownBy(
				() -> s.disposeGracefully().timeout(Duration.ofMillis(20)).block()
		);

		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> s.schedule(() -> {}));
		assertThat(isTerminated(s)).isTrue();
		assertThat(s.isDisposed()).isTrue();
	}

	@Test
	void multipleDisposeGracefully() {
		if (!shouldCheckMultipleDisposeGracefully()) {
			return;
		}

		Scheduler s = scheduler();

		CountDownLatch finishShutdownLatch = new CountDownLatch(1);
		s.schedule(() -> {
			while (true) {
				try {
					// ignore cancellation, hold graceful shutdown
					if (finishShutdownLatch.await(100, TimeUnit.MILLISECONDS)) {
						return;
					}
				} catch (InterruptedException ignored) {
				}
			}
		});

		Mono<Void> dispose1 = s.disposeGracefully();
		Mono<Void> dispose2 = s.disposeGracefully();

		StepVerifier.create(dispose1).expectTimeout(Duration.ofMillis(50)).verify();
		StepVerifier.create(dispose2).expectTimeout(Duration.ofMillis(50)).verify();

		finishShutdownLatch.countDown();

		StepVerifier.create(dispose1).verifyComplete();
		StepVerifier.create(dispose2).verifyComplete();

		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> s.schedule(() -> {}));
		assertThat(isTerminated(s)).isTrue();
		assertThat(s.isDisposed()).isTrue();
	}

	@Test
	void multipleRestarts() {
		if (!shouldCheckSupportRestart()) {
			return;
		}

		Scheduler s = scheduler();

		s.dispose();
		assertThat(s.isDisposed()).isTrue();

		// TODO: in 3.6.x: remove restart capability and this validation
		s.start();
		assertThat(s.isDisposed()).isFalse();

		s.disposeGracefully().timeout(Duration.ofMillis(20)).block();
		assertThat(s.isDisposed()).isTrue();

		s.start();
		assertThat(s.isDisposed()).isFalse();

		s.dispose();
		assertThat(s.isDisposed()).isTrue();

		s.start();
		assertThat(s.isDisposed()).isFalse();
	}

	@Test
	@Timeout(10)
	final public void workerScheduleAndDispose() throws Exception {
		Scheduler s = schedulerNotCached();
		try {
			Scheduler.Worker w = s.createWorker();

			assertThat(w.isDisposed()).isFalse();
			CountDownLatch latch = new CountDownLatch(1);
			CountDownLatch latch2 = shouldCheckDisposeTask() ? new CountDownLatch(1) : null;

			try {
				Disposable d = w.schedule(() -> {
					try{
						latch.countDown();
						if(latch2 != null && !latch2.await(10, TimeUnit.SECONDS) &&
								shouldCheckInterrupted()){
							fail("Should have interrupted");
						}
					}
					catch (InterruptedException e){
					}
				});

				latch.await();
				if(shouldCheckDisposeTask()) {
					assertThat(d.isDisposed()).isFalse();
				}
				d.dispose();
				d.dispose();//noop
			} catch (Throwable schedulingError) {
				fail("unexpected scheduling error", schedulingError);
			}

			Thread.yield();

			if(latch2 != null) {
				latch2.countDown();
			}

			Disposable[] massCancel;
			boolean hasErrors = false;
			if(shouldCheckMassWorkerDispose()) {
				int n = 10;
				massCancel = new Disposable[n];
				Throwable[] errors = new Throwable[n];
				Thread current = Thread.currentThread();
				for(int i = 0; i< n; i++){
					try {
						massCancel[i] = w.schedule(() -> {
							if(current == Thread.currentThread()){
								return;
							}
							try{
								Thread.sleep(5000);
							}
							catch (InterruptedException ie){

							}
						});
					}
					catch (RejectedExecutionException ree) {
						errors[i] = ree;
						hasErrors = true;
					}
				}
			}
			else{
				massCancel = null;
			}
			w.dispose();
			w.dispose(); //noop
			assertThat(w.isDisposed()).isTrue();

			if(massCancel != null){
				assertThat(hasErrors).as("mass cancellation errors").isFalse();
				for(Disposable _d : massCancel){
					assertThat(_d.isDisposed()).isTrue();
				}
			}

			assertThatExceptionOfType(RejectedExecutionException.class)
					.isThrownBy(() -> w.schedule(() -> {}))
					.isSameAs(Exceptions.failWithRejected());
		}
		finally {
			s.dispose();
			s.dispose();//noop
		}
	}

	@Test
	@Timeout(10)
	final public void directScheduleAndDisposeDelay() throws Exception {
		Scheduler s = schedulerNotCached();

		try {
			assertThat(s.isDisposed()).isFalse();

			if (!shouldCheckDirectTimeScheduling()) {
				assertThatExceptionOfType(RejectedExecutionException.class)
						.isThrownBy(() -> s.schedule(() -> { }, 10, TimeUnit.MILLISECONDS))
						.as("Scheduler marked as not supporting time scheduling")
						.isSameAs(Exceptions.failWithRejectedNotTimeCapable());
				return;
			}

			CountDownLatch latch = new CountDownLatch(1);
			CountDownLatch latch2 = new CountDownLatch(1);
			Disposable d = s.schedule(() -> {
				try {
					latch.countDown();
					latch2.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
				}
			}, 10, TimeUnit.MILLISECONDS);
			//will throw if not scheduled

			latch.await();
			assertThat(d.isDisposed()).isFalse();
			d.dispose();

			Thread.yield();

			latch2.countDown();

			s.dispose();
			assertThat(s.isDisposed()).isTrue();

			assertThatExceptionOfType(RejectedExecutionException.class).isThrownBy(() -> s.schedule(() -> { }));
		}
		finally {
			s.dispose();
		}
	}

	@Test
	@Timeout(10)
	final public void workerScheduleAndDisposeDelay() throws Exception {
		Scheduler s = schedulerNotCached();
		Scheduler.Worker w = s.createWorker();

		try {
			assertThat(w.isDisposed()).isFalse();

			if (!shouldCheckWorkerTimeScheduling()) {
				assertThatExceptionOfType(RejectedExecutionException.class)
						.isThrownBy(() -> w.schedule(() -> { }, 10, TimeUnit.MILLISECONDS))
						.as("Worker marked as not supporting time scheduling")
						.isSameAs(Exceptions.failWithRejectedNotTimeCapable());
				return;
			}

			CountDownLatch latch = new CountDownLatch(1);
			CountDownLatch latch2 = new CountDownLatch(1);
			Disposable d = w.schedule(() -> {
				try {
					latch.countDown();
					latch2.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
				}
			}, 10, TimeUnit.MILLISECONDS);
			//will throw if rejected

			latch.await();
			assertThat(d.isDisposed()).isFalse();
			d.dispose();

			Thread.yield();

			latch2.countDown();

			w.dispose();
			assertThat(w.isDisposed()).isTrue();

			assertThatExceptionOfType(RejectedExecutionException.class)
					.isThrownBy(() -> w.schedule(() -> { }))
					.isSameAs(Exceptions.failWithRejected());
		}
		finally {
			w.dispose();
			s.dispose();
		}
	}

	@Test
	@Timeout(10)
	final public void directScheduleAndDisposePeriod() throws Exception {
		Scheduler s = schedulerNotCached();

		try {
			assertThat(s.isDisposed()).isFalse();

			if (!shouldCheckDirectTimeScheduling()) {
				assertThatExceptionOfType(RejectedExecutionException.class)
						.isThrownBy(() -> s.schedule(() -> { }, 10, TimeUnit.MILLISECONDS))
						.as("Scheduler marked as not supporting time scheduling")
						.isSameAs(Exceptions.failWithRejectedNotTimeCapable());
				return;
			}

			CountDownLatch latch = new CountDownLatch(2);
			CountDownLatch latch2 = new CountDownLatch(1);
			Disposable d = s.schedulePeriodically(() -> {
				try {
					latch.countDown();
					if (latch.getCount() == 0) {
						latch2.await(10, TimeUnit.SECONDS);
					}
				}
				catch (InterruptedException e) {
				}
			}, 10, 10, TimeUnit.MILLISECONDS);
			//will throw if rejected

			assertThat(d.isDisposed()).isFalse();

			latch.await();
			d.dispose();

			Thread.yield();

			latch2.countDown();

			s.dispose();
			assertThat(s.isDisposed()).isTrue();

			assertThatExceptionOfType(RejectedExecutionException.class).isThrownBy(() -> s.schedule(() -> {	}));
		}
		finally {
			s.dispose();
		}
	}

	@Test
	@Timeout(10)
	final public void workerScheduleAndDisposePeriod() throws Exception {
		Scheduler s = schedulerNotCached();
		Scheduler.Worker w = s.createWorker();

		try {
			assertThat(w.isDisposed()).isFalse();

			if (!shouldCheckWorkerTimeScheduling()) {
				assertThatExceptionOfType(RejectedExecutionException.class)
						.isThrownBy(() -> w.schedule(() -> { }, 10, TimeUnit.MILLISECONDS))
						.as("Worker marked as not supporting time scheduling")
						.isSameAs(Exceptions.failWithRejectedNotTimeCapable());
				return;
			}

			CountDownLatch latch = new CountDownLatch(1);
			CountDownLatch latch2 = new CountDownLatch(1);
			Disposable c = w.schedulePeriodically(() -> {
				try {
					latch.countDown();
					latch2.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
				}
			}, 10, 10, TimeUnit.MILLISECONDS);
			Disposable d = c;
			//will throw if rejected

			latch.await();
			assertThat(d.isDisposed()).isFalse();
			d.dispose();

			Thread.yield();

			latch2.countDown();

			w.dispose();
			assertThat(w.isDisposed()).isTrue();

			assertThatExceptionOfType(RejectedExecutionException.class)
					.isThrownBy(() -> w.schedule(() -> { }))
					.isSameAs(Exceptions.failWithRejected());
		}
		finally {
			w.dispose();
			s.dispose();
		}
	}
}
