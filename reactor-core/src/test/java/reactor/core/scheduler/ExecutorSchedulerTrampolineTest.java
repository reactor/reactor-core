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

import java.util.concurrent.Executor;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Stephane Maldini
 */
public class ExecutorSchedulerTrampolineTest extends AbstractSchedulerTest {

	@Override
	protected boolean shouldCheckDisposeTask() {
		return false;
	}

	@Override
	protected Scheduler scheduler() {
		return Schedulers.fromExecutor(Runnable::run, true);
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

	@Test
	public void scanParent() {
		Executor plainExecutor = new ExecutorSchedulerTest.PlainExecutor();
		Executor scannableExecutor = new ExecutorSchedulerTest.ScannableExecutor();

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
		ExecutorScheduler.ExecutorSchedulerTrampolineWorker worker =
				new ExecutorScheduler.ExecutorSchedulerTrampolineWorker(task -> new Thread(task, "scanBuffered_Trampolining").start());

		Runnable task = () -> {
			System.out.println(Thread.currentThread().getName());
			try { Thread.sleep(1000); } catch (InterruptedException e) { e.printStackTrace(); }
		};
		try {
			//all tasks are actually executed on a single thread, starting immediately with 1st task
			worker.schedule(task);
			worker.schedule(task);
			worker.schedule(task);

			Thread.sleep(200);
			//buffered peeks into the queue of pending tasks, 1 executing and 2 pending
			assertThat(worker.scan(Scannable.Attr.BUFFERED)).isEqualTo(2);

			worker.dispose();

			assertThat(worker.scan(Scannable.Attr.BUFFERED)).isZero();
		}
		finally {
			worker.dispose();
		}
	}
}
