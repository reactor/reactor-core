package reactor.core.scheduler;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import reactor.test.AutoDisposingRule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;
import static org.awaitility.Awaitility.await;
import static reactor.core.scheduler.SchedulerMetricDecorator.TAG_SCHEDULER_ID;

@RunWith(JUnitParamsRunner.class)
public class SchedulersMetricsTest {

	final SimpleMeterRegistry simpleMeterRegistry = new SimpleMeterRegistry();

	@Rule
	public AutoDisposingRule afterTest = new AutoDisposingRule();

	@Before
	public void setUp() {
		Metrics.addRegistry(simpleMeterRegistry);
		Schedulers.enableMetrics();
	}

	@After
	public void tearDown() {
		Schedulers.disableMetrics();
		Metrics.globalRegistry.forEachMeter(Metrics.globalRegistry::remove);
		Metrics.removeRegistry(simpleMeterRegistry);
	}

	@Test
	public void metricsActivatedHasDistinctNameTags() {
		afterTest.autoDispose(Schedulers.newParallel("A", 3));
		afterTest.autoDispose(Schedulers.newParallel("B", 2));

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
		afterTest.autoDispose(Schedulers.newParallel("A", 4));
		afterTest.autoDispose(Schedulers.newParallel("A", 4));
		afterTest.autoDispose(Schedulers.newParallel("A", 3));
		afterTest.autoDispose(Schedulers.newSingle("B"));
		afterTest.autoDispose(Schedulers.newBoundedElastic(4, 100, "C").createWorker());

		assertThat(simpleMeterRegistry.getMeters()
		                              .stream()
		                              .map(m -> m.getId().getTag(TAG_SCHEDULER_ID))
		                              .distinct())
				.containsOnly(
						"parallel(4,\"A\")",
						"parallel(4,\"A\")#1",

						"parallel(3,\"A\")",

						"single(\"B\")",

						"boundedElastic(\"C\",maxThreads=4,maxTaskQueuedPerThread=100,ttl=60s)"
				);
	}

	@Test
	public void metricsActivatedHandleNamingClash() {
		afterTest.autoDispose(Schedulers.newParallel("A", 1));
		afterTest.autoDispose(Schedulers.newParallel("A", 1));
		afterTest.autoDispose(Schedulers.newParallel("A", 1));

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

		afterTest.autoDispose(Schedulers.newParallel("foo", 3));
		afterTest.autoDispose(Schedulers.fromExecutorService(anonymousExecutor1));
		afterTest.autoDispose(Schedulers.fromExecutorService(anonymousExecutor2));
		afterTest.autoDispose(Schedulers.fromExecutorService(Executors.newSingleThreadScheduledExecutor(), "testService"));
		afterTest.autoDispose(Schedulers.fromExecutorService(Executors.newSingleThreadScheduledExecutor(), "testService"));

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
		Scheduler instance = afterTest.autoDispose(Schedulers.newBoundedElastic(4, 100, "TWICE", 1));

		ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
		afterTest.autoDispose(service::shutdown);

		Schedulers.decorateExecutorService(instance, service);
		Schedulers.decorateExecutorService(instance, service);

		assertThat(simpleMeterRegistry.getMeters()
		                              .stream()
		                              .map(m -> m.getId().getTag("name"))
		                              .distinct())
				.containsOnly(
						"boundedElastic(\"TWICE\",maxThreads=4,maxTaskQueuedPerThread=100,ttl=1s)-0",
						"boundedElastic(\"TWICE\",maxThreads=4,maxTaskQueuedPerThread=100,ttl=1s)-1"
				);
	}

	@Test
	public void disablingMetricsRemovesSchedulerMeters() {
		afterTest.autoDispose(Schedulers.newParallel("A", 1));
		afterTest.autoDispose(Schedulers.newParallel("A", 1));
		afterTest.autoDispose(Schedulers.newParallel("A", 1));

		Metrics.globalRegistry.counter("foo", "tagged", "bar");

		Schedulers.disableMetrics();

		assertThat(simpleMeterRegistry.getMeters()
		                              .stream()
		                              .map(m -> m.getId().getName())
		                              .distinct())
				.containsExactly("foo");
	}

	private Object[] metricsSchedulers() {
		return new Object[] {
				new Object[] {
						(Supplier<Scheduler>) () -> Schedulers.newParallel("A", 1),
						"PARALLEL"
				},
				new Object[] {
						(Supplier<Scheduler>) () -> Schedulers.newElastic("A"),
						"ELASTIC"
				},
				new Object[] {
						(Supplier<Scheduler>) () -> Schedulers.newBoundedElastic(4, Integer.MAX_VALUE, "A"),
						"BOUNDED_ELASTIC"
				}
		};
	}

	@Test
	@Parameters(method = "metricsSchedulers")
    public void shouldReportExecutorMetrics(Supplier<Scheduler> schedulerSupplier, String type) {
		Scheduler scheduler = afterTest.autoDispose(schedulerSupplier.get());
		final int taskCount = 3;

		for (int i = 0; i < taskCount; i++) {
			scheduler.schedule(() -> {
			});
		}

		Collection<FunctionCounter> counters = simpleMeterRegistry
				.find("executor.completed")
				.tag(TAG_SCHEDULER_ID, scheduler.toString())
				.functionCounters();

		// Use Awaitility because "count" is reported "eventually"
		await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
			assertThat(counters.stream()
			                   .mapToDouble(FunctionCounter::count)
			                   .sum())
					.isEqualTo(taskCount);
		});
    }

	@Parameters(method = "metricsSchedulers")
	@Test(timeout = 10_000)
	public void shouldReportExecutionTimes(Supplier<Scheduler> schedulerSupplier, String type) {
	    Scheduler scheduler = afterTest.autoDispose(schedulerSupplier.get());
	    final int taskCount = 3;

		Phaser phaser = new Phaser(1);
		for (int i = 1; i <= taskCount; i++) {
			phaser.register();
			int delay = i * 200; //bumped delay from 20ms to make actual scheduling times more precise
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

		Collection<Timer> timers = simpleMeterRegistry
				.find("executor")
				.tag(TAG_SCHEDULER_ID, scheduler.toString())
				.timers();

		// Use Awaitility because "count" is reported "eventually"
		await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
			assertThat(timers.stream()
			                 .reduce(0d, (time, timer) -> time + timer.totalTime(TimeUnit.MILLISECONDS), Double::sum))
					.as("total durations")
					.isEqualTo(600 + 400 + 200, offset(30.0d));
			assertThat(timers.stream().mapToLong(Timer::count).sum())
					.as("count")
					.isEqualTo(taskCount);
		});
	}

	@Test
	public void shouldRemoveOnShutdown() throws Exception {
		int ttl = 1;
		Scheduler scheduler = afterTest.autoDispose(Schedulers.newElastic("A", ttl));
		String schedulerName = scheduler.toString();

		Scheduler.Worker worker0 = scheduler.createWorker();
		Scheduler.Worker worker1 = scheduler.createWorker();

		Predicate<Meter.Id> schedulerPredicate = it -> {
			return schedulerName.equals(it.getTag(TAG_SCHEDULER_ID));
		};

		assertThat(simpleMeterRegistry.getMeters())
				.extracting(Meter::getId)
				.anyMatch(schedulerPredicate);

		worker1.dispose();

		await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
			((ElasticScheduler) scheduler).eviction();

			List<Meter> meters = simpleMeterRegistry.getMeters();
			assertThat(meters)
					.extracting(Meter::getId)
					.anyMatch(it -> (schedulerName + "-0").equals(it.getTag("name")))
					.noneMatch(it -> (schedulerName + "-1").equals(it.getTag("name")));
		});

		scheduler.dispose();

		assertThat(simpleMeterRegistry.getMeters())
				.extracting(Meter::getId)
				.noneMatch(schedulerPredicate);
	}

	@Test
	public void shouldRemoveAllOnDispose() {
		Scheduler scheduler = afterTest.autoDispose(Schedulers.newParallel("A", 2));

		Predicate<Meter.Id> meterPredicate = it -> {
			return scheduler.toString().equals(it.getTag(TAG_SCHEDULER_ID));
		};

		assertThat(simpleMeterRegistry.getMeters())
				.extracting(Meter::getId)
				.anyMatch(meterPredicate);

		scheduler.dispose();

		assertThat(simpleMeterRegistry.getMeters())
				.extracting(Meter::getId)
				.noneMatch(meterPredicate);
	}
}
