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

import reactor.core.Disposable;
import reactor.core.Disposables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;
import static org.awaitility.Awaitility.await;
import static reactor.core.scheduler.SchedulerMetricDecorator.TAG_SCHEDULER_ID;

public class SchedulersMetricsTest {

	final SimpleMeterRegistry simpleMeterRegistry = new SimpleMeterRegistry();
	final Disposable.Composite toCleanUp = Disposables.composite();

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
		toCleanUp.dispose(); //dispose all the resources added to this composite
	}

	/**
	 * Add a newly constructed resource to the automatic cleanup list and return it.
	 */
	private <D extends Disposable> D autoCleanup(D resource) {
		toCleanUp.add(resource);
		return resource;
	}

	@Test
	public void metricsActivatedHasDistinctNameTags() {
		autoCleanup(Schedulers.newParallel("A", 3));
		autoCleanup(Schedulers.newParallel("B", 2));

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
		autoCleanup(Schedulers.newParallel("A", 4));
		autoCleanup(Schedulers.newParallel("A", 4));
		autoCleanup(Schedulers.newParallel("A", 3));
		autoCleanup(Schedulers.newSingle("B"));
		autoCleanup(Schedulers.newElastic("C").createWorker());

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
		autoCleanup(Schedulers.newParallel("A", 1));
		autoCleanup(Schedulers.newParallel("A", 1));
		autoCleanup(Schedulers.newParallel("A", 1));

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

	//see https://github.com/reactor/reactor-core/issues/1739
	@Test
	public void fromExecutorServiceSchedulerId() throws InterruptedException {
		ScheduledExecutorService anonymousExecutor1 = Executors.newSingleThreadScheduledExecutor();
		ScheduledExecutorService anonymousExecutor2 = Executors.newSingleThreadScheduledExecutor();

		String anonymousId1 = "anonymousExecutor@" + Integer.toHexString(System.identityHashCode(anonymousExecutor1));
		String anonymousId2 = "anonymousExecutor@" + Integer.toHexString(System.identityHashCode(anonymousExecutor2));

		autoCleanup(Schedulers.newParallel("foo", 3));
		autoCleanup(Schedulers.fromExecutorService(anonymousExecutor1));
		autoCleanup(Schedulers.fromExecutorService(anonymousExecutor2));
		autoCleanup(Schedulers.fromExecutorService(Executors.newSingleThreadScheduledExecutor(), "testService"));
		autoCleanup(Schedulers.fromExecutorService(Executors.newSingleThreadScheduledExecutor(), "testService"));

		assertThat(
				simpleMeterRegistry.getMeters()
				                   .stream()
				                   .map(m -> m.getId().getTag("name"))
				                   .distinct()
		)
				.containsExactlyInAnyOrder(
						"parallel(3,\"foo\")-0",
						"parallel(3,\"foo\")-2",
						"parallel(3,\"foo\")-1",
						"fromExecutorService(" + anonymousId1 + ")-0",
						"fromExecutorService(" + anonymousId2 + ")-0",
						"fromExecutorService(testService)-0",
						"fromExecutorService(testService)#1-0"
				);
	}

	@Test
	public void decorateTwiceWithSameSchedulerInstance() {
		Scheduler instance = autoCleanup(Schedulers.newElastic("TWICE", 1));

		ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
		autoCleanup(service::shutdown);

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
		autoCleanup(Schedulers.newParallel("A", 1));
		autoCleanup(Schedulers.newParallel("A", 1));
		autoCleanup(Schedulers.newParallel("A", 1));

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

		// Use Awaitility because "count" is reported "eventually"
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

		// Use Awaitility because "count" is reported "eventually"
		await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
			assertThat(timer)
					.isNotNull()
					.satisfies(it -> {
						assertThat(it.count()).as("count").isEqualTo(taskCount);
						assertThat(it.max(TimeUnit.MILLISECONDS))
								.as("min")
								.isEqualTo(60, offset(10.0d));
					});
		});
	}
}
