package reactor.core.scheduler;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import reactor.core.util.BackpressureUtils;

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
	 * @return the Cancellable that let's one cancel this particular delayed task.
	 */
	Runnable schedule(Runnable task, long delay, TimeUnit unit);
	
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
	Runnable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit);
	
	default Runnable schedule(Runnable task, Duration delay) {
		long s = BackpressureUtils.multiplyCap(delay.getSeconds(), 1_000_000_000);
		long d = BackpressureUtils.addCap(s, delay.getNano());
		
		return schedule(task, d, TimeUnit.NANOSECONDS);
	}

	default Runnable schedulePeriodically(Runnable task, Duration initialDelay, Duration period) {
		long s0 = BackpressureUtils.multiplyCap(initialDelay.getSeconds(), 1_000_000_000);
		long d0 = BackpressureUtils.addCap(s0, initialDelay.getNano());
		
		long s1 = BackpressureUtils.multiplyCap(period.getSeconds(), 1_000_000_000);
		long d1 = BackpressureUtils.addCap(s1, period.getNano());

		return schedulePeriodically(task, d0, d1, TimeUnit.NANOSECONDS);
	}
	
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
		 * @return the Cancellable that let's one cancel this particular delayed task.
		 */
		Runnable schedule(Runnable task, long delay, TimeUnit unit);
		
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
		 * @return the Cancellable that let's one cancel this particular delayed task.
		 */
		Runnable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit);
		
		default Runnable schedule(Runnable task, Duration delay) {
			long s = BackpressureUtils.multiplyCap(delay.getSeconds(), 1_000_000_000);
			long d = BackpressureUtils.addCap(s, delay.getNano());
			
			return schedule(task, d, TimeUnit.NANOSECONDS);
		}

		default Runnable schedulePeriodically(Runnable task, Duration initialDelay, Duration period) {
			long s0 = BackpressureUtils.multiplyCap(initialDelay.getSeconds(), 1_000_000_000);
			long d0 = BackpressureUtils.addCap(s0, initialDelay.getNano());
			
			long s1 = BackpressureUtils.multiplyCap(period.getSeconds(), 1_000_000_000);
			long d1 = BackpressureUtils.addCap(s1, period.getNano());

			return schedulePeriodically(task, d0, d1, TimeUnit.NANOSECONDS);
		}

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
