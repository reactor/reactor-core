/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.observability.micrometer;

import java.util.concurrent.TimeUnit;

import io.micrometer.common.docs.KeyName;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.docs.DocumentedMeter;

import reactor.core.scheduler.Scheduler;

//
public enum SchedulerMeters implements DocumentedMeter {

	/**
	 * {@link Counter} that increments by one each time a task is submitted (via any of the
	 * schedule methods on both {@link Scheduler} and {@link Scheduler.Worker}).
	 * <p>
	 * Note that there are actually 4 counters, which can be differentiated by the {@link SubmittedTags#SUBMISSION} tag.
	 * The sum of all these can thus be compared with the {@link #TASKS_COMPLETED} counter.
	 */
	SUBMITTED {
		@Override
		public KeyName[] getKeyNames() {
			return SubmittedTags.values();
		}

		@Override
		public String getName() {
			return "%s" + "scheduler.tasks.submitted";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.COUNTER;
		}
	},

	/**
	 * {@link LongTaskTimer} reflecting tasks currently running. Note that this reflects all types of
	 * active tasks, including tasks scheduled {@link Scheduler#schedule(Runnable, long, TimeUnit) with a delay}
	 * or {@link Scheduler#schedulePeriodically(Runnable, long, long, TimeUnit) periodically} (each
	 * iteration being considered an active task).
	 */
	TASKS_ACTIVE {
		@Override
		public String getName() {
			return "%sscheduler.tasks.active";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.LONG_TASK_TIMER;
		}
	},

	/**
	 * {@link Timer} reflecting tasks that have finished execution. Note that this reflects all types of
	 * active tasks, including tasks scheduled {@link Scheduler#schedule(Runnable, long, TimeUnit) with a delay}
	 * or {@link Scheduler#schedulePeriodically(Runnable, long, long, TimeUnit) periodically} (each
	 * iteration being considered a separate completed task).
	 */
	TASKS_COMPLETED {
		@Override
		public String getName() {
			return "%sscheduler.tasks.completed";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.TIMER;
		}
	},

	/**
	 * {@link LongTaskTimer} reflecting tasks that were submitted for immediate execution but
	 * couldn't be started immediately because the scheduler is already at max capacity.
	 * Note that only immediate submissions via {@link Scheduler#schedule(Runnable)} and
	 * {@link Scheduler.Worker#schedule(Runnable)} are considered.
	 */
	TASKS_PENDING {
		@Override
		public String getName() {
			return "%sscheduler.tasks.pending";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.LONG_TASK_TIMER;
		}

	}
	;

	/**
	 * Tag for the {@link SchedulerMeters#SUBMITTED} meter.
	 */
	public enum SubmittedTags implements KeyName {

		/**
		 * The type of submission:
		 * <ul>
		 *   <li>{@link #SUBMISSION_DIRECT} for {@link Scheduler#schedule(Runnable)}</li>
		 *   <li>{@link #SUBMISSION_DELAYED} for {@link Scheduler#schedule(Runnable, long, TimeUnit)}</li>
		 *   <li>{@link #SUBMISSION_PERIODIC_INITIAL} for {@link Scheduler#schedulePeriodically(Runnable, long, long, TimeUnit)} after the initial delay</li>
		 *   <li>{@link #SUBMISSION_PERIODIC_ITERATION} for {@link Scheduler#schedulePeriodically(Runnable, long, long, TimeUnit)} further periodic iterations</li>
		 * </ul>
		 */
		SUBMISSION {
			@Override
			public String asString() {
				return "submission.type";
			}
		};

		/**
		 * {@link Counter} that increments by one each time a task is submitted for immediate execution
		 * (ie. {@link Scheduler#schedule(Runnable)} or {@link Scheduler.Worker#schedule(Runnable)}).
		 */
		public static final String SUBMISSION_DIRECT             = "direct";
		/**
		 * {@link Counter} that increments by one each time a task is submitted with a delay
		 * (ie. {@link Scheduler#schedule(Runnable, long, TimeUnit)}
		 * or {@link Scheduler.Worker#schedule(Runnable, long, TimeUnit)}).
		 */
		public static final String SUBMISSION_DELAYED            = "delayed";
		/**
		 * {@link Counter} that increments when a task is initially submitted with a period
		 * (ie. {@link Scheduler#schedulePeriodically(Runnable, long, long, TimeUnit)}
		 * or {@link Scheduler.Worker#schedulePeriodically(Runnable, long, long, TimeUnit)}). This isn't
		 * incremented on further iterations of the periodic task.
		 */
		public static final String SUBMISSION_PERIODIC_INITIAL   = "periodic_initial";
		/**
		 * {@link Counter} that increments by one each time a task is re-executed due to the periodic
		 * nature of {@link Scheduler#schedulePeriodically(Runnable, long, long, TimeUnit)}
		 * or {@link Scheduler.Worker#schedulePeriodically(Runnable, long, long, TimeUnit)} (ie. iterations
		 * past the initial one).
		 */
		public static final String SUBMISSION_PERIODIC_ITERATION = "periodic_iteration";

	}

}
