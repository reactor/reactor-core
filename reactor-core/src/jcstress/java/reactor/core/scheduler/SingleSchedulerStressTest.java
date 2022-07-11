package reactor.core.scheduler;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.ZZZ_Result;
import org.openjdk.jcstress.infra.results.Z_Result;

public abstract class SingleSchedulerStressTest {

	@JCStressTest
	@Outcome(id = {"true"}, expect = Expect.ACCEPTABLE, desc = "Task scheduled after racing restart")
	@State
	public static class StartDisposeStressTest {

		final SingleScheduler scheduler = new SingleScheduler(Thread::new);
		{
			scheduler.start();
		}

		@Actor
		public void restart1() {
			scheduler.disposeGracefully(Duration.ofMillis(100)).block(Duration.ofMillis(100));
			scheduler.start();
		}

		@Actor
		public void restart2() {
			scheduler.disposeGracefully(Duration.ofMillis(100)).block(Duration.ofMillis(100));
			scheduler.start();
		}

		@Arbiter
		public void arbiter(Z_Result r) {
			// At this stage, at least one actor called scheduler.start(),
			// so we should be able to execute a task.
			final CountDownLatch latch = new CountDownLatch(1);
			if (scheduler.isDisposed()) {
				return;
			}
			scheduler.schedule(() -> {
				r.r1 = true;
				latch.countDown();
			});
			try {
				r.r1 = latch.await(100, TimeUnit.MILLISECONDS);
			} catch (InterruptedException ignored) {
			}
			scheduler.dispose();
		}
	}

	@JCStressTest
	@Outcome(id = {"true, true, true"}, expect = Expect.ACCEPTABLE,
			desc = "Both time out, task gets rejected, scheduler disposed eventually")
	@State
	public static class DisposeGracefullyStressTest {
		final SingleScheduler scheduler = new SingleScheduler(Thread::new);
		final CountDownLatch arbiterLatch = new CountDownLatch(1);
		{
			scheduler.start();
			// Schedule a task that disallows graceful closure until the arbiter kicks in
			// to make sure that actors fail while waiting.
			scheduler.schedule(() -> {
				while (true) {
					try {
						if (arbiterLatch.await(20, TimeUnit.MILLISECONDS)) {
							return;
						}
					}
					catch (InterruptedException ignored) {
					}
				}
			});
		}

		@Actor
		public void disposeGracefully1(ZZZ_Result r) {
			long start = System.nanoTime();
			try {
				scheduler.disposeGracefully(Duration.ofMillis(20)).block();
			} catch (Exception e) {
				long duration = System.nanoTime() - start;
				// Validate that the wait took non-zero time.
				r.r1 = (e.getCause() instanceof TimeoutException) && Duration.ofNanos(duration).toMillis() > 15;
			}
		}

		@Actor
		public void disposeGracefully2(ZZZ_Result r) {
			long start = System.nanoTime();
			try {
				scheduler.disposeGracefully(Duration.ofMillis(20)).block();
			} catch (Exception e) {
				long duration = System.nanoTime() - start;
				// Validate that the wait took non-zero time.
				r.r2 = (e.getCause() instanceof TimeoutException) && Duration.ofNanos(duration).toMillis() > 15;
			}
		}

		@Arbiter
		public void arbiter(ZZZ_Result r) {
			// Release the task blocking graceful closure.
			arbiterLatch.countDown();
			try {
				scheduler.schedule(() -> {});
			} catch (RejectedExecutionException e) {
				scheduler.disposeGracefully(Duration.ofMillis(20)).block();
				r.r3 = scheduler.isDisposed() && scheduler.state.executor.isTerminated();
			}
		}
	}
}
