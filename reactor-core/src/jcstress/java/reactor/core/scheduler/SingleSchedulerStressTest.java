package reactor.core.scheduler;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.ZZZ_Result;

public abstract class SingleSchedulerStressTest {

	public static class StartDisposeStressTest {

		final SingleScheduler scheduler = new SingleScheduler(Thread::new);

		public void start() {
			scheduler.start();
		}

		public void disposeGracefully() {
			scheduler.disposeGracefully(Duration.ofMillis(100)).block();
		}

		public void arbiter() {
			scheduler.isDisposed();
		}
	}

	@JCStressTest
	@Outcome(id = {"true, true, true"}, expect = Expect.ACCEPTABLE, desc = "Both finish in time, scheduled disposed")
	@State
	public static class DisposeGracefullyStressTest {
		final SingleScheduler scheduler = new SingleScheduler(Thread::new);
		final CountDownLatch  latch = new CountDownLatch(1);
		{
			scheduler.start();
			scheduler.schedule(() -> {
				while (true) {
					try {
						if (latch.await(50, TimeUnit.MILLISECONDS)) {
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
				scheduler.disposeGracefully(Duration.ofMillis(70)).block();
			} catch (Exception ignored) {
			}
			long duration = System.nanoTime() - start;
//			latch.countDown();
			r.r1 = Duration.ofNanos(duration).toMillis() > 50;
		}

		@Actor
		public void disposeGracefully2(ZZZ_Result r) {
			long start = System.nanoTime();
			try {
				scheduler.disposeGracefully(Duration.ofMillis(70)).block();
			} catch (Exception ignored) {
			}
			long duration = System.nanoTime() - start;
//			latch.countDown();
			r.r2 = Duration.ofNanos(duration).toMillis() > 50;
		}

		@Arbiter
		public void arbiter(ZZZ_Result r) {
			latch.countDown();
			try {
				scheduler.schedule(() -> {});
			} catch (RejectedExecutionException e) {
				scheduler.disposeGracefully(Duration.ofMillis(50)).block();
				r.r3 = scheduler.isDisposed() && scheduler.state.executor.isTerminated();
			}
		}
	}

//	@JCStressTest
//	@Outcome(id = {"true, true, true"}, expect = Expect.ACCEPTABLE, desc = "Both finish" +
//			" in time, scheduled disposed")
//	@State
//	public static class DisposeGracefullyStressTest2 {
//		final SingleScheduler scheduler = new SingleScheduler(Thread::new);
//		final CountDownLatch  latch = new CountDownLatch(2);
//		{
//			scheduler.start();
//			scheduler.schedule(() -> {
//				while (true) {
//					try {
//						if (latch.await(50, TimeUnit.MILLISECONDS)) {
//							return;
//						}
//					}
//					catch (InterruptedException ignored) {
//					}
//				}
//			});
//		}
//
//		public void disposeGracefully1() {
//			scheduler.disposeGracefully(Duration.ofMillis(100)).block();
//			latch.countDown();
//		}
//
//		public void disposeGracefully2() {
//			scheduler.disposeGracefully(Duration.ofMillis(100)).block();
//			latch.countDown();
//		}
//
//		public void arbiter(ZZZ_Result r) {
//			scheduler.isDisposed();
//			try {
//				scheduler.schedule(() -> {});
//			} catch (RejectedExecutionException e) {
//				r.r3 = scheduler.isDisposed();
//			}
//		}
//	}

}
