/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.test.util.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class WorkerTaskTest {

	private static final int RACE_DEFAULT_LOOPS = 2500;

	@Test
	public void dispose() {
		Disposable.Composite set = Disposables.composite();
		WorkerTask run = new WorkerTask(() -> {}, set);
		set.add(run);

		assertThat(run.isDisposed()).isFalse();

		set.dispose();

		assertThat(run.isDisposed()).isTrue();
	}

	@Test
	public void disposeRun() {
		Disposable.Composite set = Disposables.composite();
		WorkerTask run = new WorkerTask(() -> {}, set);
		set.add(run);

		assertThat(run.isDisposed()).isFalse();

		run.dispose();
		run.dispose();

		assertThat(run.isDisposed()).isTrue();
	}

	@Test
	public void setFutureCancelRace() {
		for (int i = 0; i < RACE_DEFAULT_LOOPS; i++) {
			Disposable.Composite set = Disposables.composite();
			final WorkerTask run = new WorkerTask(() -> {}, set);
			set.add(run);

			final FutureTask<Object> ft = new FutureTask<>(() -> {
			}, 0);

			Runnable r1 = () -> run.setFuture(ft);

			Runnable r2 = run::dispose;

			RaceTestUtils.race(r1, r2);

			assertThat(set.size()).isZero();
		}
	}

	@Test
	public void setFutureRunRace() {
		for (int i = 0; i < RACE_DEFAULT_LOOPS; i++) {
			Disposable.Composite set = Disposables.composite();
			final WorkerTask run = new WorkerTask(() -> {}, set);
			set.add(run);

			final FutureTask<Object> ft = new FutureTask<>(() -> {
			}, 0);

			Runnable r1 = () -> run.setFuture(ft);

			Runnable r2 = run::run;

			RaceTestUtils.race(r1, r2);

			assertThat(set.size()).isZero();
		}
	}

	@Test
	public void disposeRace() {
		for (int i = 0; i < RACE_DEFAULT_LOOPS; i++) {
			Disposable.Composite set = Disposables.composite();
			final WorkerTask run = new WorkerTask(() -> {}, set);
			set.add(run);

			Runnable r1 = run::dispose;

			RaceTestUtils.race(r1, r1);

			assertThat(set.size()).isZero();
		}
	}

	@Test
	public void runDispose() {
		for (int i = 0; i < RACE_DEFAULT_LOOPS; i++) {
			Disposable.Composite set = Disposables.composite();
			final WorkerTask run = new WorkerTask(() -> {}, set);
			set.add(run);

			Runnable r1 = run::call;

			Runnable r2 = run::dispose;

			RaceTestUtils.race(r1, r2);

			assertThat(set.size()).isZero();
		}
	}

	@Test
	public void pluginCrash() {
		Thread.currentThread().setUncaughtExceptionHandler((t, e) -> {
			throw new IllegalStateException("Second");
		});

		Disposable.Composite set = Disposables.composite();
		final WorkerTask run = new WorkerTask(() -> {
			throw new IllegalStateException("First");
		}, set);
		set.add(run);

		try {
			run.run();

			fail("Should have thrown!");
		} catch (IllegalStateException ex) {
			assertThat(ex).hasMessage("Second");
		} finally {
			Thread.currentThread().setUncaughtExceptionHandler(null);
		}
		assertThat(run.isDisposed()).as("isDisposed").isTrue();

		assertThat(set.size()).isZero();
	}

	@Test
	public void crashReported() {
		List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
		Schedulers.onHandleError((thread, error) -> errors.add(error));
		try {
			Disposable.Composite set = Disposables.composite();
			final WorkerTask run = new WorkerTask(() -> {
				throw new IllegalStateException("First");
			}, set);
			set.add(run);

			run.run();

			assertThat(run.isDisposed()).as("isDisposed").isTrue();

			assertThat(set.size()).isZero();

			assertThat(errors)
					.hasSize(1);
			assertThat(errors.get(0))
					.isInstanceOf(IllegalStateException.class)
					.hasMessage("First");
		} finally {
			Schedulers.resetOnHandleError();
		}
	}

	@Test
	public void withoutParentDisposed() {
		WorkerTask run = new WorkerTask(() -> {}, null);
		run.dispose();
		run.call();
	}

	@Test
	public void withParentDisposed() {
		WorkerTask run = new WorkerTask(() -> {}, Disposables.composite());
		run.dispose();
		run.call();
	}

	@Test
	public void withFutureDisposed() {
		WorkerTask run = new WorkerTask(() -> {}, null);
		run.setFuture(new FutureTask<Void>(() -> {}, null));
		run.dispose();
		run.call();
	}

	@Test
	public void withFutureDisposed2() {
		WorkerTask run = new WorkerTask(() -> {}, null);
		run.dispose();
		run.setFuture(new FutureTask<Void>(() -> {}, null));
		run.call();
	}

	@Test
	public void withFutureDisposed3() {
		WorkerTask run = new WorkerTask(() -> {}, null);
		run.dispose();
		WorkerTask.THREAD.set(run, Thread.currentThread());
		run.setFuture(new FutureTask<Void>(() -> {}, null));
		run.call();
	}

	@Test
	public void runFuture() {
		for (int i = 0; i < RACE_DEFAULT_LOOPS; i++) {
			Disposable.Composite set = Disposables.composite();
			final WorkerTask run = new WorkerTask(() -> {}, set);
			set.add(run);

			final FutureTask<Void> ft = new FutureTask<>(() -> {
			}, null);

			Runnable r1 = run::call;

			Runnable r2 = () -> run.setFuture(ft);

			RaceTestUtils.race(r1, r2);
		}
	}

	@Test
	public void syncWorkerCancelRace() {
		for (int i = 0; i < RACE_DEFAULT_LOOPS; i++) {
			final Disposable.Composite set = Disposables.composite();
			final AtomicBoolean interrupted = new AtomicBoolean();
			final AtomicInteger sync = new AtomicInteger(2);
			final AtomicInteger syncb = new AtomicInteger(2);

			Runnable r0 = () -> {
				set.dispose();
				if (sync.decrementAndGet() != 0) {
					while (sync.get() != 0) { }
				}
				if (syncb.decrementAndGet() != 0) {
					while (syncb.get() != 0) { }
				}
				for (int j = 0; j < 1000; j++) {
					if (Thread.currentThread().isInterrupted()) {
						interrupted.set(true);
						break;
					}
				}
			};

			final WorkerTask run = new WorkerTask(r0, set);
			set.add(run);

			final FutureTask<Void> ft = new FutureTask<>(run, null);

			Runnable r2 = () -> {
				if (sync.decrementAndGet() != 0) {
					while (sync.get() != 0) { }
				}
				run.setFuture(ft);
				if (syncb.decrementAndGet() != 0) {
					while (syncb.get() != 0) { }
				}
			};

			RaceTestUtils.race(ft, r2);

			assertThat(interrupted.get()).as("The task was interrupted").isFalse();
		}
	}

	@Test
	public void disposeAfterRun() {
		final WorkerTask run = new WorkerTask(() -> {}, null);

		run.run();
		assertThat((Future<?>) WorkerTask.FUTURE.get(run)).isEqualTo(WorkerTask.FINISHED);

		run.dispose();
		assertThat((Future<?>) WorkerTask.FUTURE.get(run)).isEqualTo(WorkerTask.FINISHED);
	}

	@Test
	public void syncDisposeIdempotent() {
		final WorkerTask run = new WorkerTask(() -> {}, null);
		WorkerTask.THREAD.set(run, Thread.currentThread());

		run.dispose();
		assertThat((Future<?>) WorkerTask.FUTURE.get(run)).isEqualTo(WorkerTask.SYNC_CANCELLED);
		run.dispose();
		assertThat((Future<?>) WorkerTask.FUTURE.get(run)).isEqualTo(WorkerTask.SYNC_CANCELLED);
		run.run();
		assertThat((Future<?>) WorkerTask.FUTURE.get(run)).isEqualTo(WorkerTask.SYNC_CANCELLED);
	}

	@Test
	public void asyncDisposeIdempotent() {
		final WorkerTask run = new WorkerTask(() -> {}, null);

		run.dispose();
		assertThat((Future<?>) WorkerTask.FUTURE.get(run))
				.isEqualTo(WorkerTask.ASYNC_CANCELLED);

		run.dispose();
		assertThat((Future<?>) WorkerTask.FUTURE.get(run))
				.isEqualTo(WorkerTask.ASYNC_CANCELLED);

		run.run();
		assertThat((Future<?>) WorkerTask.FUTURE.get(run))
				.isEqualTo(WorkerTask.ASYNC_CANCELLED);
	}


	@Test
	public void noParentIsDisposed() {
		WorkerTask run = new WorkerTask(() -> {}, null);
		assertThat(run.isDisposed()).as("not yet disposed").isFalse();
		run.run();
		assertThat(run.isDisposed()).as("isDisposed").isTrue();
	}

	@Test
	public void withParentIsDisposed() {
		Disposable.Composite set = Disposables.composite();
		WorkerTask run = new WorkerTask(() -> {}, set);
		set.add(run);

		assertThat(run.isDisposed()).as("not yet disposed").isFalse();

		run.run();
		assertThat(run.isDisposed()).as("isDisposed").isTrue();

		assertThat(set.remove(run)).isFalse();
	}

}
