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

import io.micrometer.common.docs.KeyName;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.docs.DocumentedMeter;

/**
 * Meters and tags used by {@link TimedScheduler}.
 */
public enum SchedulerMeters implements DocumentedMeter {

	/**
	 * Counter that increments by one each time a task is submitted (via any of the
	 * schedule methods on both Scheduler and Scheduler.Worker).
	 * <p>
	 * Note that there are actually 4 counters, which can be differentiated by the SubmittedTags#SUBMISSION tag.
	 * The sum of all these can thus be compared with the TASKS_COMPLETED counter.
	 */
	SUBMITTED {
		@Override
		public KeyName[] getKeyNames() {
			return SubmittedTags.values();
		}

		@Override
		public String getName() {
			return "scheduler.tasks.submitted";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.COUNTER;
		}
	},

	/**
	 * LongTaskTimer reflecting tasks currently running. Note that this reflects all types of
	 * active tasks, including tasks scheduled with a delay or periodically (each
	 * iteration being considered an active task).
	 */
	TASKS_ACTIVE {
		@Override
		public String getName() {
			return "scheduler.tasks.active";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.LONG_TASK_TIMER;
		}
	},

	/**
	 * Timer reflecting tasks that have finished execution. Note that this reflects all types of
	 * active tasks, including tasks  with a delay or periodically (each iteration being considered
	 * a separate completed task).
	 */
	TASKS_COMPLETED {
		@Override
		public String getName() {
			return "scheduler.tasks.completed";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.TIMER;
		}
	},

	/**
	 * LongTaskTimer reflecting tasks that were submitted for immediate execution but
	 * couldn't be started immediately because the scheduler is already at max capacity.
	 * Note that only immediate submissions via Scheduler#schedule(Runnable) and
	 * Scheduler.Worker#schedule(Runnable) are considered.
	 */
	TASKS_PENDING {
		@Override
		public String getName() {
			return "scheduler.tasks.pending";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.LONG_TASK_TIMER;
		}

	}
	;

	/**
	 * Tag for the SchedulerMeters#SUBMITTED meter.
	 */
	public enum SubmittedTags implements KeyName {

		/**
		 * The type of submission:
		 * <ul>
		 *   <li>#SUBMISSION_DIRECT for Scheduler#schedule(Runnable)</li>
		 *   <li>#SUBMISSION_DELAYED for Scheduler#schedule(Runnable, long, TimeUnit)</li>
		 *   <li>#SUBMISSION_PERIODIC_INITIAL for Scheduler#schedulePeriodically(Runnable, long, long, TimeUnit) after the initial delay</li>
		 *   <li>#SUBMISSION_PERIODIC_ITERATION for Scheduler#schedulePeriodically(Runnable, long, long, TimeUnit) further periodic iterations</li>
		 * </ul>
		 */
		SUBMISSION {
			@Override
			public String asString() {
				return "submission.type";
			}
		};

		/**
		 * Counter that increments by one each time a task is submitted for immediate execution
		 * (ie. Scheduler#schedule(Runnable) or Scheduler.Worker#schedule(Runnable)).
		 */
		public static final String SUBMISSION_DIRECT             = "direct";
		/**
		 * Counter that increments by one each time a task is submitted with a delay
		 * (ie. Scheduler#schedule(Runnable, long, TimeUnit)
		 * or Scheduler.Worker#schedule(Runnable, long, TimeUnit)).
		 */
		public static final String SUBMISSION_DELAYED            = "delayed";
		/**
		 * Counter that increments when a task is initially submitted with a period
		 * (ie. Scheduler#schedulePeriodically(Runnable, long, long, TimeUnit)
		 * or Scheduler.Worker#schedulePeriodically(Runnable, long, long, TimeUnit)). This isn't
		 * incremented on further iterations of the periodic task.
		 */
		public static final String SUBMISSION_PERIODIC_INITIAL   = "periodic_initial";
		/**
		 * Counter that increments by one each time a task is re-executed due to the periodic
		 * nature of Scheduler#schedulePeriodically(Runnable, long, long, TimeUnit)
		 * or Scheduler.Worker#schedulePeriodically(Runnable, long, long, TimeUnit) (ie. iterations
		 * past the initial one).
		 */
		public static final String SUBMISSION_PERIODIC_ITERATION = "periodic_iteration";

	}

}
