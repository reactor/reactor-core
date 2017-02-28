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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import reactor.core.Cancellation;
import reactor.core.Disposable;

/**
 * Provides an abstract asynchronous boundary to operators.
 * <p>
 * Implementations that use an underlying {@link ExecutorService} or
 * {@link ScheduledExecutorService} should instantiate it through a {@link Supplier}
 * passed through the relevant {@link Schedulers} hook
 * ({@link Schedulers#decorateExecutorService(String, Supplier)} or
 * {@link Schedulers#decorateScheduledExecutorService(String, Supplier)}).
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 */
public interface Scheduler extends Disposable {

	/**
	 * Check if a Scheduler instance is "time-capable", that is to say that it can
	 * perform {@link #schedule(Runnable, long, TimeUnit)} and
	 * {@link #schedulePeriodically(Runnable, long, long, TimeUnit)}
	 * operations.
	 * <p>
	 * The {@link Scheduler} default is to return false and have the above operations
	 * immediately return the pre-disposed {@link Scheduler#NOT_TIMED} instance.
	 *
	 * @return true if the scheduler is time-capable, false otherwise.
	 */
		default boolean isTimeCapable() {
			return this instanceof TimedScheduler;
		}

	/**
	 * Schedules the given task on this scheduler non-delayed execution.
	 *
	 * <p>
	 * This method is safe to be called from multiple threads but there are no
	 * ordering guarantees between tasks.
	 *
	 * @param task the task to execute
	 *
	 * @return the {@link Cancellation} instance that let's one cancel this particular task.
	 * If the {@link Scheduler} has been shut down, the {@link #REJECTED} {@link Cancellation} instance is returned.
	 */
	Cancellation schedule(Runnable task);

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
	 * @return the {@link Cancellation} that let's one cancel this particular delayed task,
	 * or {@link #NOT_TIMED} if the Scheduler is not capable of scheduling periodically.
	 */
	default Cancellation schedule(Runnable task, long delay, TimeUnit unit) {
		return NOT_TIMED;
	}

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
	 * @return the {@link Cancellation} that let's one cancel this particular delayed task,
	 * or {@link #NOT_TIMED} if the Scheduler is not capable of scheduling periodically.
	 */
	default Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
		return NOT_TIMED;
	}

	/**
	 * Returns the "current time" notion of this scheduler.
	 * @param unit the target unit of the current time
	 * @return the current time value in the target unit of measure
	 */
	default long now(TimeUnit unit) {
		return unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
	}
	
	/**
	 * Creates a worker of this Scheduler that executed task in a strict
	 * FIFO order, guaranteed non-concurrently with each other.
	 * <p>
	 * Once the Worker is no longer in use, one should call dispose() on it to
	 * release any resources the particular Scheduler may have used.
	 * 
	 * <p>Tasks scheduled with this worker run in FIFO order and strictly non-concurrently, but
	 * there are no ordering guarantees between different Workers created from the same
	 * Scheduler.
	 * 
	 * @return the Worker instance.
	 */
	Worker createWorker();

	/**
	 * Instructs this Scheduler to release all resources and reject
	 * any new tasks to be executed.
	 *
	 * <p>The operation is thread-safe but one should avoid using
	 * start() and dispose() concurrently as it would non-deterministically
	 * leave the Scheduler in either active or inactive state.
	 *
	 * <p>The Scheduler may choose to ignore this instruction.
	 *
	 */
	default void dispose() {
		shutdown();
	}

	/**
	 * Instructs this Scheduler to prepare itself for running tasks
	 * directly or through its Workers.
	 *
	 * <p>The operation is thread-safe but one should avoid using
	 * start() and dispose() concurrently as it would non-deterministically
	 * leave the Scheduler in either active or inactive state.
	 */
	default void start() {
	}

	/**
	 * Instructs this Scheduler to release all resources and reject
	 * any new tasks to be executed.
	 *
	 * <p>The operation is thread-safe but one should avoid using
	 * start() and shutdown() concurrently as it would non-deterministically
	 * leave the Scheduler in either active or inactive state.
	 *
	 * @deprecated move the implementation to {@link #dispose()} and call dispose from shutdown.
	 * Will be removed in 3.1.0
	 */
	@Deprecated
	default void shutdown() {

	}

	/**
	 * A worker representing an asynchronous boundary that executes tasks in
	 * a FIFO order, guaranteed non-concurrently with respect to each other.
	 *
	 * @author Stephane Maldini
	 * @author Simon Baslé
	 */
	interface Worker extends Disposable {

		/**
		 * Check if a Worker instance is "time-capable", that is to say that it can
		 * perform {@link #schedule(Runnable, long, TimeUnit)} and
		 * {@link #schedulePeriodically(Runnable, long, long, TimeUnit)}
		 * operations.
		 * <p>
		 * The {@link Worker} default in {@link Scheduler} is to return false and have
		 * the above operations immediately return the pre-disposed
		 * {@link Scheduler#NOT_TIMED} instance.
		 *
		 * @return true if the worker is time-capable, false otherwise.
		 */
		default boolean isTimeCapable() {
			return false;
		}

		/**
		 * Schedules the task on this worker.
		 * @param task the task to schedule
		 * @return the {@link Cancellation} instance that let's one cancel this particular task.
		 * If the Scheduler has been shut down, the {@link #REJECTED} {@link Cancellation} instance is returned.
		 */
		Cancellation schedule(Runnable task);

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
		 * @return the {@link Cancellation} that let's one cancel this particular delayed task,
		 * or {@link #NOT_TIMED} if the Worker is not capable of scheduling with delay.
		 */
		default Cancellation schedule(Runnable task, long delay, TimeUnit unit) {
			return NOT_TIMED;
		}

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
		 * @return the {@link Cancellation} that let's one cancel this particular delayed task,
		 * or {@link #NOT_TIMED} if the Worker is not capable of scheduling periodically.
		 */
		default Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
			return NOT_TIMED;
		}

		/**
		 * Instructs this worker to cancel all pending tasks, all running tasks in 
		 * a best-effort manner, reject new tasks and
		 * release any resources associated with it.
		 *
		 * @deprecated move the implementation to {@link #dispose()} and call dispose from shutdown.
		 * Will be removed in 3.1.0
		 */
		@Deprecated
		void shutdown();

		/**
		 * Instructs this worker to cancel all pending tasks, all running tasks in
		 * a best-effort manner, reject new tasks and
		 * release any resources associated with it.
		 *
		 */
		@Override
		default void dispose() {
			shutdown();
		}
	}
	
	/**
	 * Returned by the schedule() methods if the Scheduler or the Worker has ben shut down.
	 */
	Disposable REJECTED = new RejectedDisposable();

	/**
	 * Returned by the schedule() methods if the Scheduler is incapable of deferring tasks
	 * to a future point in time / executing tasks at a time interval.
	 */
	Disposable NOT_TIMED = new RejectedDisposable();
}
