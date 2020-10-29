/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.scheduler;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import reactor.core.Exceptions;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Stephane Maldini
 */
public class ExecutorSchedulerTest extends AbstractSchedulerTest {

	@Override
	protected boolean shouldCheckDisposeTask() {
		return false;
	}

	@Override
	protected boolean shouldCheckDirectTimeScheduling() {
		return false;
	}

	@Override
	protected boolean shouldCheckWorkerTimeScheduling() {
		return false;
	}

	@Override
	protected boolean shouldCheckSupportRestart() {
		return false;
	}

	@Override
	protected Scheduler scheduler() {
		return Schedulers.fromExecutor(Runnable::run);
	}

	@Test
	public void directAndWorkerTimeSchedulingRejected() {
		Scheduler scheduler = scheduler();
		Scheduler.Worker worker = scheduler.createWorker();
		try {
			assertThatExceptionOfType(RejectedExecutionException.class)
					.isThrownBy(() -> scheduler.schedule(() -> { }, 100, TimeUnit.MILLISECONDS))
					.isSameAs(Exceptions.failWithRejectedNotTimeCapable());
			assertThatExceptionOfType(RejectedExecutionException.class)
					.isThrownBy(() -> scheduler.schedulePeriodically(() -> { }, 100, 100, TimeUnit.MILLISECONDS))
					.isSameAs(Exceptions.failWithRejectedNotTimeCapable());

			assertThatExceptionOfType(RejectedExecutionException.class)
					.isThrownBy(() -> worker.schedule(() -> { }, 100, TimeUnit.MILLISECONDS))
					.isSameAs(Exceptions.failWithRejectedNotTimeCapable());
			assertThatExceptionOfType(RejectedExecutionException.class)
					.isThrownBy(() -> worker.schedulePeriodically(() -> { }, 100, 100, TimeUnit.MILLISECONDS))
					.isSameAs(Exceptions.failWithRejectedNotTimeCapable());
		}
		finally {
			worker.dispose();
		}
	}

	@Test
	public void failingExecutorRejects() {
		final IllegalStateException boom = new IllegalStateException("boom");
		ExecutorScheduler scheduler = new ExecutorScheduler(
				task -> { throw boom;},
				false
		);

		AtomicBoolean done = new AtomicBoolean();

		assertThatExceptionOfType(RejectedExecutionException.class)
				.isThrownBy(() -> scheduler.schedule(() -> done.set(true)))
				.withCause(boom);

		assertThat(done.get()).isFalse();
	}

	@Test
	public void failingPlainExecutorIsNotTerminated() {
		AtomicInteger count = new AtomicInteger();
		final IllegalStateException boom = new IllegalStateException("boom");
		ExecutorScheduler scheduler = new ExecutorScheduler(
				task -> {
					if (count.incrementAndGet() % 2 == 0)
						throw boom;
					},
				false
		);
		assertThatCode(() -> scheduler.schedule(() -> {}))
				.as("initial-no rejection")
				.doesNotThrowAnyException();

		assertThatExceptionOfType(RejectedExecutionException.class)
				.as("second-transient rejection")
				.isThrownBy(() -> scheduler.schedule(() -> {}))
				.withCause(boom);

		assertThatCode(() -> scheduler.schedule(() -> {}))
				.as("third-no rejection")
				.doesNotThrowAnyException();

		assertThat(count).hasValue(3);
	}

	@Test
	public void failingExecutorServiceIsNotTerminated() {
		AtomicInteger count = new AtomicInteger();
		final IllegalStateException boom = new IllegalStateException("boom");
		ExecutorService service = new AbstractExecutorService() {

			boolean shutdown;

			@Override
			public void shutdown() {
				shutdown = true;
			}

			@NotNull
			@Override
			public List<Runnable> shutdownNow() {
				return Collections.emptyList();
			}

			@Override
			public boolean isShutdown() {
				return shutdown;
			}

			@Override
			public boolean isTerminated() {
				return shutdown;
			}

			@Override
			public boolean awaitTermination(long timeout, @NotNull TimeUnit unit)
					throws InterruptedException {
				return false;
			}

			@Override
			public void execute(@NotNull Runnable command) {
				if (count.incrementAndGet() % 2 == 0)
					throw boom;
			}
		};
		ExecutorScheduler scheduler = new ExecutorScheduler(service, false);

		assertThatCode(() -> scheduler.schedule(() -> {}))
				.as("initial-no rejection")
				.doesNotThrowAnyException();

		assertThatExceptionOfType(RejectedExecutionException.class)
				.as("second-transient rejection")
				.isThrownBy(() -> scheduler.schedule(() -> {}))
				.withCause(boom);

		assertThatCode(() -> scheduler.schedule(() -> {}))
				.as("third-no rejection")
				.doesNotThrowAnyException();

		assertThat(count).hasValue(3);
	}

	@Test
	public void failingAndShutDownExecutorServiceIsTerminated() {
		final IllegalStateException boom = new IllegalStateException("boom");
		ExecutorService service = new AbstractExecutorService() {

			boolean shutdown;

			@Override
			public void shutdown() {
				shutdown = true;
			}

			@NotNull
			@Override
			public List<Runnable> shutdownNow() {
				return Collections.emptyList();
			}

			@Override
			public boolean isShutdown() {
				return shutdown;
			}

			@Override
			public boolean isTerminated() {
				return shutdown;
			}

			@Override
			public boolean awaitTermination(long timeout, @NotNull TimeUnit unit)
					throws InterruptedException {
				return false;
			}

			@Override
			public void execute(@NotNull Runnable command) {
				if (shutdown) throw boom;
				shutdown = true;
			}
		};
		ExecutorScheduler scheduler = new ExecutorScheduler(service, false);

		assertThatCode(() -> scheduler.schedule(() -> {}))
				.as("initial-no rejection")
				.doesNotThrowAnyException();

		assertThatExceptionOfType(RejectedExecutionException.class)
				.as("second-transient rejection")
				.isThrownBy(() -> scheduler.schedule(() -> {}))
				.withCause(boom);

		assertThatExceptionOfType(RejectedExecutionException.class)
				.as("third scheduler terminated rejection")
				.isThrownBy(() -> scheduler.schedule(() -> {}))
				.isSameAs(Exceptions.failWithRejected())
				.withNoCause();
	}

	static final class ScannableExecutor implements Executor, Scannable {

		@Override
		public void execute(@NotNull Runnable command) {
			command.run();
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CAPACITY) return 123;
			if (key == Attr.BUFFERED) return 1024;
			if (key == Attr.NAME) return toString();
			return null;
		}

		@Override
		public String toString() {
			return "scannableExecutor";
		}
	}

	static final class PlainExecutor implements Executor {
		@Override
		public void execute(@NotNull Runnable command) {
			command.run();
		}

		@Override
		public String toString() {
			return "\"plain\"";
		}
	}

	@Test
	public void scanName() {
		Executor executor = new PlainExecutor();

		Scheduler noTrampoline = Schedulers.fromExecutor(executor, false);
		Scheduler withTrampoline = Schedulers.fromExecutor(executor, true);

		Scheduler.Worker workerNoTrampoline = noTrampoline.createWorker();
		Scheduler.Worker workerWithTrampoline = withTrampoline.createWorker();

		try {
			assertThat(Scannable.from(noTrampoline).scan(Scannable.Attr.NAME))
					.as("noTrampoline")
					.isEqualTo("fromExecutor(\"plain\")");

			assertThat(Scannable.from(withTrampoline).scan(Scannable.Attr.NAME))
					.as("withTrampoline")
					.isEqualTo("fromExecutor(\"plain\",trampolining)");

			assertThat(Scannable.from(workerNoTrampoline).scan(Scannable.Attr.NAME))
					.as("workerNoTrampoline")
					.isEqualTo("fromExecutor(\"plain\").worker");

			assertThat(Scannable.from(workerWithTrampoline).scan(Scannable.Attr.NAME))
					.as("workerWithTrampoline")
					.isEqualTo("fromExecutor(\"plain\",trampolining).worker");
		}
		finally {
			noTrampoline.dispose();
			withTrampoline.dispose();
			workerNoTrampoline.dispose();
			workerWithTrampoline.dispose();
		}
	}

	@Test
	public void scanParent() {
		Executor plainExecutor = new PlainExecutor();
		Executor scannableExecutor = new ScannableExecutor();

		Scheduler.Worker workerScannableParent =
				new ExecutorScheduler.ExecutorSchedulerWorker(scannableExecutor);
		Scheduler.Worker workerPlainParent =
				new ExecutorScheduler.ExecutorSchedulerWorker(plainExecutor);

		try {
			assertThat(Scannable.from(workerScannableParent).scan(Scannable.Attr.PARENT))
					.as("workerScannableParent")
					.isSameAs(scannableExecutor);

			assertThat(Scannable.from(workerPlainParent).scan(Scannable.Attr.PARENT))
					.as("workerPlainParent")
					.isNull();
		}
		finally {
			workerPlainParent.dispose();
			workerScannableParent.dispose();
		}
	}


	@Test
	public void scanBuffered() throws InterruptedException {
		ExecutorScheduler.ExecutorSchedulerWorker worker =
				new ExecutorScheduler.ExecutorSchedulerWorker(task -> new Thread(task, "scanBuffered_NotTrampolining").start());

		Runnable task = () -> {
			System.out.println(Thread.currentThread().getName());
			try { Thread.sleep(1000); } catch (InterruptedException e) { e.printStackTrace(); }
		};
		try {
			//the tasks are all added to the list and started on a dedicated thread
			worker.schedule(task);
			worker.schedule(task);
			worker.schedule(task);

			Thread.sleep(200);
			//buffered looks at the list of executing and pending tasks, 1 executing and 2 pending
			assertThat(worker.scan(Scannable.Attr.BUFFERED)).isEqualTo(3);

			worker.dispose();

			assertThat(worker.scan(Scannable.Attr.BUFFERED)).isZero();
		}
		finally {
			worker.dispose();
		}
	}
}
