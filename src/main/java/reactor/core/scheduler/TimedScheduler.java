/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.TimeUnit;

import reactor.core.Cancellation;

/**
 * Provides an abstract, timed asychronous boundary to operators.
 */
public interface TimedScheduler extends Scheduler {
	
	/**
	 * Schedules the execution of the given task with the given delay amount.
	 * 
	 * <p>
	 * This method is safe to be called from multiple threads but there are no
	 * ordering guarantees between tasks.
	 * 
	 * @param task the task to schedule
	 * @param delay the delay amount, non-positive values indicate non-delayed scheduling
	 * @param unit the unit of measure of the delay amount
	 * @return the Cancellation that let's one cancel this particular delayed task.
	 */
	Cancellation schedule(Runnable task, long delay, TimeUnit unit);
	
	/**
	 * Schedules a periodic execution of the given task with the given initial delay and period.
	 * 
	 * <p>
	 * This method is safe to be called from multiple threads but there are no
	 * ordering guarantees between tasks.
	 * 
	 * <p>
	 * The periodic execution is at a fixed rate, that is, the first execution will be after the initial
	 * delay, the second after initialDelay + period, the third after initialDelay + 2 * period, and so on.
	 * 
	 * @param task the task to schedule
	 * @param initialDelay the initial delay amount, non-positive values indicate non-delayed scheduling
	 * @param period the period at which the task should be re-executed
	 * @param unit the unit of measure of the delay amount
	 * @return the Cancellable that let's one cancel this particular delayed task.
	 */
	Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit);
	
	/**
	 * Returns the "current time" notion of this scheduler.
	 * @param unit the target unit of the current time
	 * @return the current time value in the target unit of measure
	 */
	default long now(TimeUnit unit) {
		return unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
	}
	
	@Override
	TimedWorker createWorker();
	
	interface TimedWorker extends Worker {
		
		/**
		 * Schedules the execution of the given task with the given delay amount.
		 * 
		 * <p>
		 * This method is safe to be called from multiple threads and tasks are executed in
		 * some total order. Two tasks scheduled at a same time with the same delay will be
		 * ordered in FIFO order if the schedule() was called from the same thread or
		 * in arbitrary order if the schedule() was called from different threads.
		 * 
		 * @param task the task to schedule
		 * @param delay the delay amount, non-positive values indicate non-delayed scheduling
		 * @param unit the unit of measure of the delay amount
		 * @return the Cancellation that let's one cancel this particular delayed task.
		 */
		Cancellation schedule(Runnable task, long delay, TimeUnit unit);
		
		/**
		 * Schedules a periodic execution of the given task with the given initial delay and period.
		 * 
		 * <p>
		 * This method is safe to be called from multiple threads.
		 * 
		 * <p>
		 * The periodic execution is at a fixed rate, that is, the first execution will be after the initial
		 * delay, the second after initialDelay + period, the third after initialDelay + 2 * period, and so on.
		 * 
		 * @param task the task to schedule
		 * @param initialDelay the initial delay amount, non-positive values indicate non-delayed scheduling
		 * @param period the period at which the task should be re-executed
		 * @param unit the unit of measure of the delay amount
		 * @return the Cancellation that let's one cancel this particular delayed task.
		 */
		Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit);
		
		/**
		 * Returns the "current time" notion of this scheduler.
		 * @param unit the target unit of the current time
		 * @return the current time value in the target unit of measure
		 */
		default long now(TimeUnit unit) {
			return unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
		}
	}
}
