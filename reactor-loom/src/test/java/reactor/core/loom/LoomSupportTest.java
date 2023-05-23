package reactor.core.loom;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

class LoomSupportTest {


	@Test
	public void testWithVirtualThreads() throws InterruptedException {
//		Scheduler scheduler = Schedulers.boundedElastic();
//		scheduler.schedulePeriodically(() -> System.out.println("hello world" + Thread.currentThread()),
//				100,
//				100,
//				TimeUnit.MILLISECONDS);

		Thread.sleep(10000);
		// 1 tweak bounded elastic at runtime and decide whether to run on loom
		// thread or on
		// kernel threads

		// 2 add mapAsync which is going to be executed on BE, but once BE runs on Loom
		// mapAsync start being non-blocking

		// 3 add separate extra module

//		Scheduler scheduler = Schedulers.boundedElastic(); // VirtualThreadsExecutre
//		// used instead for blocking tasks
		Flux.range(0, 1000)
			.log("before")
			.mapBlocking(a -> {
				try {
					Thread.sleep(1000);
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

				return a + 1;
			})
			.log("after")
			.doOnNext(__ -> System.out.println(Thread.currentThread()))
			.take(50)
			.blockLast();

	}
}