/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Provides an abstract asynchronous boundary to operators.
 * <p>
 * Implementations that use an underlying {@link ExecutorService} or
 * {@link ScheduledExecutorService} should decorate it with the relevant {@link Schedulers} hook
 * ({@link Schedulers#decorateExecutorService(Scheduler, ScheduledExecutorService)}.
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 */
public interface Scheduler extends Disposable {

	/**
	 * Schedules the non-delayed execution of the given task on this scheduler.
	 *
	 * <p>
	 * This method is safe to be called from multiple threads but there are no
	 * ordering guarantees between tasks.
	 *
	 * @param task the task to execute
	 *
	 * @return the {@link Disposable} instance that let's one cancel this particular task.
	 * If the {@link Scheduler} has been shut down, throw a {@link RejectedExecutionException}.
	 */
	Disposable schedule(Runnable task);

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
	 * @return the {@link Disposable} that let's one cancel this particular delayed task,
	 * or throw a {@link RejectedExecutionException} if the Scheduler is not capable of scheduling with delay.
	 */
	default Disposable schedule(Runnable task, long delay, TimeUnit unit) {
		throw Exceptions.failWithRejectedNotTimeCapable();
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
	 * @return the {@link Disposable} that let's one cancel this particular delayed task,
	 * or throw a {@link RejectedExecutionException} if the Scheduler is not capable of scheduling periodically.
	 */
	default Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
		throw Exceptions.failWithRejectedNotTimeCapable();
	}

	/**
	 * Returns the "current time" notion of this scheduler.
	 *
	 * <p>
	 *     <strong>Implementation Note:</strong> The default implementation uses {@link System#currentTimeMillis()}
	 *     when requested with a {@code TimeUnit} of {@link TimeUnit#MILLISECONDS milliseconds} or coarser, and
	 *     {@link System#nanoTime()} otherwise. As a consequence, results should not be interpreted as absolute timestamps
	 *     in the latter case, only monotonicity inside the current JVM can be expected.
	 * </p>
	 * @param unit the target unit of the current time
	 * @return the current time value in the target unit of measure
	 */
	default long now(TimeUnit unit) {
		if (unit.compareTo(TimeUnit.MILLISECONDS) >= 0) {
			return unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
		} else {
			return unit.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
		}
	}
	
	/**
	 * Creates a worker of this Scheduler.
	 * <p>
	 * Once the Worker is no longer in use, one should call dispose() on it to
	 * release any resources the particular Scheduler may have used.
	 *
	 * It depends on the implementation, but Scheduler Workers should usually run tasks in
	 * FIFO order. Some implementations may entirely delegate the scheduling to an
	 * underlying structure (like an {@link ExecutorService}).
	 *
	 * @return the Worker instance.
	 */
	Worker createWorker();

	/**
	 * Instructs this Scheduler to release all resources and reject
	 * any new tasks to be executed.
	 *
	 * <p>The operation is thread-safe.
	 *
	 * <p>The Scheduler may choose to ignore this instruction.
	 * <p>When used in combination with {@link #disposeGracefully()}
	 * there are no guarantees that all resources will be forcefully shutdown.
	 * When a graceful disposal has started, the references to the underlying
	 * {@link java.util.concurrent.Executor}s might have already been lost.
	 */
	default void dispose() {
	}

	/**
	 * Lazy variant of {@link #dispose()} that also allows for graceful cleanup
	 * of underlying resources.
	 * <p>It is advised to apply a {@link Mono#timeout(Duration)} operator to the
	 * resulting {@link Mono}.
	 * <p>The returned {@link Mono} can be {@link Mono#retry(long) retried} in case of
	 * {@link java.util.concurrent.TimeoutException timeout errors}. It can also be
	 * followed by a call to {@link #dispose()} to issue a forceful shutdown of
	 * underlying resources.
	 *
	 * @return {@link Mono} which upon subscription initiates the graceful dispose
	 * procedure. If the disposal is successful, the returned {@link Mono} completes
	 * without an error.
	 */
	default Mono<Void> disposeGracefully() {
		return Mono.fromRunnable(this::dispose);
	}

	/**
	 * Instructs this Scheduler to prepare itself for running tasks
	 * directly or through its Workers.
	 *
	 * <p>The operation is thread-safe but one should avoid using
	 * start() and dispose() concurrently as it would non-deterministically
	 * leave the Scheduler in either active or inactive state.
	 *
	 * @deprecated Use {@link #init()} instead. The use of this method is discouraged.
	 * Some implementations allowed restarting a Scheduler, while others did not. One
	 * of the issues with restarting is that checking
	 * {@link #isDisposed() the disposed state} is unreliable in concurrent scenarios.
	 * @see #init()
	 */
	@Deprecated
	default void start() {
	}

	/**
	 * Instructs this Scheduler to prepare itself for running tasks
	 * directly or through its {@link Worker}s.
	 *
	 * <p>Implementations are encouraged to throw an exception if this method is called
	 * after the scheduler has been disposed via {@link #dispose()}
	 * or {@link #disposeGracefully()}.
	 */
	default void init() {
		start();
	}

	/**
	 * A worker representing an asynchronous boundary that executes tasks.
	 *
	 * @author Stephane Maldini
	 * @author Simon Baslé
	 */
	interface Worker extends Disposable {

		/**
		 * Schedules the task for immediate execution on this worker.
		 * @param task the task to schedule
		 * @return the {@link Disposable} instance that let's one cancel this particular task.
		 * If the Scheduler has been shut down, a {@link RejectedExecutionException} is thrown.
		 */
		Disposable schedule(Runnable task);

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
		 * @return the {@link Disposable} that let's one cancel this particular delayed task,
		 * or throw a {@link RejectedExecutionException} if the Worker is not capable of scheduling with delay.
		 */
		default Disposable schedule(Runnable task, long delay, TimeUnit unit) {
			throw Exceptions.failWithRejectedNotTimeCapable();
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
		 * @return the {@link Disposable} that let's one cancel this particular delayed task,
		 * or throw a {@link RejectedExecutionException} if the Worker is not capable of scheduling periodically.
		 */
		default Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
			throw Exceptions.failWithRejectedNotTimeCapable();
		}

	}
}
