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

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.assertj.core.api.Assumptions;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler.Worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Stephane Maldini
 */
public class ImmediateSchedulerTest extends AbstractSchedulerTest {

	@Override
	protected Scheduler scheduler() {
		return Schedulers.immediate();
	}

	@Override
	protected boolean shouldCheckDisposeTask() {
		return false;
	}

	@Override
	protected boolean shouldCheckMassWorkerDispose() {
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
	@Test
	public void restartSupport() {
		//immediate is a bit weird: disposing doesn't make sense any more than restarting
		Assumptions.assumeThat(false).as("immediate cannot be either disposed nor restarted").isFalse();
	}

	@Test
	public void directAndWorkerTimeSchedulingRejected() {
		Scheduler scheduler = scheduler();
		Worker worker = scheduler.createWorker();
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
	public void scanScheduler() {
		ImmediateScheduler s = (ImmediateScheduler) Schedulers.immediate();

		assertThat(s.scan(Scannable.Attr.NAME)).isEqualTo(Schedulers.IMMEDIATE);
		//don't test TERMINATED as this would shutdown the only instance
	}

	@Test
	public void scanWorker() {
		Worker worker = Schedulers.immediate().createWorker();
		Scannable s = (Scannable) worker;

		assertThat(s.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(s.scan(Scannable.Attr.CANCELLED)).isFalse();
		assertThat(s.scan(Scannable.Attr.NAME)).isEqualTo(Schedulers.IMMEDIATE + ".worker");

		worker.dispose();
		assertThat(s.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(s.scan(Scannable.Attr.CANCELLED)).isTrue();
	}
}
