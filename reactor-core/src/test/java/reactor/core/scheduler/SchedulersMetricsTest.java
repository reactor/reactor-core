package reactor.core.scheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.Disposables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;
import static org.awaitility.Awaitility.await;
import static reactor.core.scheduler.SchedulerMetricDecorator.TAG_SCHEDULER_ID;

public class SchedulersMetricsTest {

	final SimpleMeterRegistry simpleMeterRegistry = new SimpleMeterRegistry();

	final Disposable.Composite disposables = Disposables.composite();

	@Before
	public void setUp() {
		Metrics.addRegistry(simpleMeterRegistry);
		Schedulers.enableMetrics();
	}

	@After
	public void tearDown() {
		disposables.dispose();
		Schedulers.disableMetrics();
		Metrics.globalRegistry.forEachMeter(Metrics.globalRegistry::remove);
		Metrics.removeRegistry(simpleMeterRegistry);
	}

	@Test
	public void metricsActivatedHasDistinctNameTags() {
		Schedulers.newParallel("A", 3);
		Schedulers.newParallel("B", 2);

		assertThat(simpleMeterRegistry.getMeters()
		                              .stream()
		                              .map(m -> m.getId().getTag("name"))
		                              .distinct())
				.containsOnly(
						"parallel(3,\"A\")-0",
						"parallel(3,\"A\")-1",
						"parallel(3,\"A\")-2",

						"parallel(2,\"B\")-0",
						"parallel(2,\"B\")-1"
				);
	}

	@Test
	public void metricsActivatedHasDistinctSchedulerIdTags() {
		Schedulers.newParallel("A", 4);
		Schedulers.newParallel("A", 4);
		Schedulers.newParallel("A", 3);
		Schedulers.newSingle("B");
		Schedulers.newElastic("C").createWorker();

		assertThat(simpleMeterRegistry.getMeters()
		                              .stream()
		                              .map(m -> m.getId().getTag(TAG_SCHEDULER_ID))
		                              .distinct())
				.containsOnly(
						"parallel(4,\"A\")",
						"parallel(4,\"A\")#1",

						"parallel(3,\"A\")",

						"single(\"B\")",

						"elastic(\"C\")"
				);
	}

	@Test
	public void metricsActivatedHandleNamingClash() {
		Schedulers.newParallel("A", 1);
		Schedulers.newParallel("A", 1);
		Schedulers.newParallel("A", 1);

		assertThat(simpleMeterRegistry.getMeters()
		                              .stream()
		                              .map(m -> m.getId().getTag("name"))
		                              .distinct())
				.containsOnly(
						"parallel(1,\"A\")-0",
						"parallel(1,\"A\")#1-0",
						"parallel(1,\"A\")#2-0"
				);
	}

	@Test
	public void decorateTwiceWithSameSchedulerInstance() {
		Scheduler instance = Schedulers.newElastic("TWICE", 1);

		ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

		Schedulers.decorateExecutorService(instance, service);
		Schedulers.decorateExecutorService(instance, service);

		assertThat(simpleMeterRegistry.getMeters()
		                              .stream()
		                              .map(m -> m.getId().getTag("name"))
		                              .distinct())
				.containsOnly(
						"elastic(\"TWICE\")-0",
						"elastic(\"TWICE\")-1"
				);
	}

	@Test
	public void disablingMetricsRemovesSchedulerMeters() {
		Schedulers.newParallel("A", 1);
		Schedulers.newParallel("A", 1);
		Schedulers.newParallel("A", 1);

		Metrics.globalRegistry.counter("foo", "tagged", "bar");

		Schedulers.disableMetrics();

		assertThat(simpleMeterRegistry.getMeters()
		                              .stream()
		                              .map(m -> m.getId().getName())
		                              .distinct())
				.containsExactly("foo");
	}

	@Test
    public void shouldReportExecutorMetrics() {
		Scheduler scheduler = Schedulers.newParallel("A", 1);
		disposables.add(scheduler);

		final int taskCount = 3;

		for (int i = 0; i < taskCount; i++) {
			scheduler.schedule(() -> {
			});
		}

		FunctionCounter counter = simpleMeterRegistry
				.find("executor.completed")
				.tag(TAG_SCHEDULER_ID, scheduler.toString())
				.functionCounter();

		// Use Awaitility instead of Phaser because "count" is reported "eventually"
		await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
			assertThat(counter)
					.isNotNull()
					.satisfies(it -> {
						assertThat(it.count())
								.as("count")
								.isEqualTo(taskCount);
					});
		});
    }

	@Test(timeout = 10_000)
	public void shouldReportExecutionTimes() {
		Scheduler scheduler = Schedulers.newParallel("A", 1);
		disposables.add(scheduler);

		final int taskCount = 3;

		Phaser phaser = new Phaser(1);
		for (int i = 1; i <= taskCount; i++) {
			phaser.register();
			int delay = i * 20;
			scheduler.schedule(() -> {
				try {
					Thread.sleep(delay);
					phaser.arriveAndDeregister();
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			});
		}
		phaser.arriveAndAwaitAdvance();

		Timer timer = simpleMeterRegistry
				.find("executor")
				.tag(TAG_SCHEDULER_ID, scheduler.toString())
				.timer();

		assertThat(timer)
				.isNotNull()
				.satisfies(it -> {
					assertThat(it.count()).as("count").isEqualTo(taskCount);
					assertThat(it.max(TimeUnit.MILLISECONDS))
							.as("min")
							.isEqualTo(60, offset(10.0d));
				});
	}
}
