package reactor.core.scheduler;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class DelegatingScheduledExecutorService implements ScheduledExecutorService {

	final ScheduledExecutorService scheduledExecutorService;

	DelegatingScheduledExecutorService(ScheduledExecutorService service) {
		scheduledExecutorService = service;
	}

	@Override
	public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
		return scheduledExecutorService.schedule(command, delay, unit);
	}

	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
		return scheduledExecutorService.schedule(callable, delay, unit);
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
		return scheduledExecutorService.scheduleAtFixedRate(command, initialDelay, period, unit);
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
		return scheduledExecutorService.scheduleWithFixedDelay(command, initialDelay, delay, unit);
	}

	@Override
	public void shutdown() {
		scheduledExecutorService.shutdown();
	}

	@Override
	public List<Runnable> shutdownNow() {
		return scheduledExecutorService.shutdownNow();
	}

	@Override
	public boolean isShutdown() {
		return scheduledExecutorService.isShutdown();
	}

	@Override
	public boolean isTerminated() {
		return scheduledExecutorService.isTerminated();
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return scheduledExecutorService.awaitTermination(timeout, unit);
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		return scheduledExecutorService.submit(task);
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		return scheduledExecutorService.submit(task, result);
	}

	@Override
	public Future<?> submit(Runnable task) {
		return scheduledExecutorService.submit(task);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return scheduledExecutorService.invokeAll(tasks);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {
		return scheduledExecutorService.invokeAll(tasks, timeout, unit);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		return scheduledExecutorService.invokeAny(tasks);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		return scheduledExecutorService.invokeAny(tasks, timeout, unit);
	}

	@Override
	public void execute(Runnable command) {
		scheduledExecutorService.execute(command);
	}
}
