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

import reactor.core.Cancellation;

/**
 * Provides an abstract asynchronous boundary to operators.
 */
public interface Scheduler {
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
	 * Creates a worker of this Scheduler that executed task in a strict
	 * FIFO order, guaranteed non-concurrently with each other.
	 * <p>
	 * Once the Worker is no longer in use, one should call shutdown() on it to
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
	 * Instructs this Scheduler to prepare itself for running tasks
	 * directly or through its Workers.
	 * 
	 * <p>The operation is thread-safe but one should avoid using
	 * start() and shutdown() concurrently as it would non-deterministically
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
	 */
	default void shutdown() {
		
	}
	
	/**
	 * A worker representing an asynchronous boundary that executes tasks in
	 * a FIFO order, guaranteed non-concurrently with respect to each other. 
	 */
	interface Worker {
		
		/**
		 * Schedules the task on this worker.
		 * @param task the task to schedule
		 * @return the Cancellation instance that let's one cancel this particular task.
		 * If the Scheduler has been shut down, the {@link #REJECTED} Cancellation instance is returned.
		 */
		Cancellation schedule(Runnable task);
		
		/**
		 * Instructs this worker to cancel all pending tasks, all running tasks in 
		 * a best-effort manner, reject new tasks and
		 * release any resources associated with it.
		 */
		void shutdown();
	}
	
	/**
	 * Returned by the schedule() methods if the Scheduler or the Worker has ben shut down.
	 */
	Cancellation REJECTED = new Cancellation() {
		@Override
		public void dispose() {
			// deliberately no-op
		}
		
		@Override
		public String toString() {
			return "Rejected task";
		}
	};
}
