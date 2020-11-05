package reactor.core.scheduler;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import reactor.test.AutoDisposingExtension;
import reactor.util.Metrics;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static reactor.core.scheduler.SchedulerMetricDecorator.TAG_SCHEDULER_ID;

public class SchedulersMetricsTest {


	@RegisterExtension
	public AutoDisposingExtension afterTest = new AutoDisposingExtension();

	private MeterRegistry registry;
	private MeterRegistry previousRegistry;

	@BeforeEach
	public void setUp() {
		registry = new SimpleMeterRegistry();
		previousRegistry = Metrics.MicrometerConfiguration.useRegistry(registry);
		Schedulers.enableMetrics();
	}

	@AfterEach
	public void tearDown() {
		Schedulers.disableMetrics();
		registry.close();
		Metrics.MicrometerConfiguration.useRegistry(previousRegistry);
	}

	@Test
	public void metricsActivatedHasDistinctNameTags() {
		afterTest.autoDispose(Schedulers.newParallel("A", 3));
		afterTest.autoDispose(Schedulers.newParallel("B", 2));

		assertThat(registry.getMeters()
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

		assertThat(registry.getMeters()
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

		assertThat(registry.getMeters()
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
				registry.getMeters()
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

		assertThat(registry.getMeters()
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

		final Counter otherCounter = registry.counter("foo", "tagged", "bar");
		afterTest.autoDispose(() -> registry.remove(otherCounter));

		Schedulers.disableMetrics();

		assertThat(registry.getMeters()
		                              .stream()
		                              .map(m -> m.getId().getName())
		                              .distinct())
				.containsExactly("foo");
	}

	static Object[] metricsSchedulers() {
		return new Object[] {
				new Object[] {
						(Supplier<Scheduler>) () -> Schedulers.newParallel("A", 1),
						"PARALLEL"
				},
				new Object[] {
						(Supplier<Scheduler>) () -> {
							@SuppressWarnings("deprecation") // To be removed in 3.5 alongside Schedulers.newElastic()
							Scheduler newElastic = Schedulers.newElastic("A");
							return newElastic;
						},
						"ELASTIC"
				},
				new Object[] {
						(Supplier<Scheduler>) () -> Schedulers.newBoundedElastic(4, Integer.MAX_VALUE, "A"),
						"BOUNDED_ELASTIC"
				}
		};
	}

	@ParameterizedTest
	@MethodSource("metricsSchedulers")
    public void shouldReportExecutorMetrics(Supplier<Scheduler> schedulerSupplier, String type) {
		Scheduler scheduler = afterTest.autoDispose(schedulerSupplier.get());
		final int taskCount = 3;

		for (int i = 0; i < taskCount; i++) {
			scheduler.schedule(() -> {
			});
		}

		Collection<FunctionCounter> counters = registry
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

	@ParameterizedTest
	@MethodSource("metricsSchedulers")
	@Timeout(10)
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

		Collection<Timer> timers = registry
				.find("executor")
				.tag(TAG_SCHEDULER_ID, scheduler.toString())
				.timers();

		// Use Awaitility because "count" is reported "eventually"
		await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
			assertThat(timers.stream()
			                 .reduce(0d, (time, timer) -> time + timer.totalTime(TimeUnit.MILLISECONDS), Double::sum))
					.as("total durations")
					.isEqualTo(600 + 400 + 200, offset(50d));
			assertThat(timers.stream().mapToLong(Timer::count).sum())
					.as("count")
					.isEqualTo(taskCount);
		});
	}

	@Test
	public void shouldRemoveOnShutdown() throws Exception {
		int ttl = 1;
		Scheduler scheduler = afterTest.autoDispose(Schedulers.newBoundedElastic(2, 2, "A", ttl));
		String schedulerName = scheduler.toString();

		Scheduler.Worker worker0 = scheduler.createWorker();
		Scheduler.Worker worker1 = scheduler.createWorker();

		Predicate<Meter.Id> schedulerPredicate = it -> {
			return schedulerName.equals(it.getTag(TAG_SCHEDULER_ID));
		};

		assertThat(registry.getMeters())
				.extracting(Meter::getId)
				.anyMatch(schedulerPredicate);

		// Explicitly do *not* dispose worker0 and assert that metrics from it still live below
		worker1.dispose();

		await().atMost(ttl*5, TimeUnit.SECONDS).untilAsserted(() -> {
			List<Meter> meters = registry.getMeters();
			assertThat(meters)
					.extracting(Meter::getId)
					.anyMatch(it -> (schedulerName + "-0").equals(it.getTag("name")))
					.noneMatch(it -> (schedulerName + "-1").equals(it.getTag("name")));
		});

		scheduler.dispose();

		assertThat(registry.getMeters())
				.extracting(Meter::getId)
				.noneMatch(schedulerPredicate);
	}

	@Test
	public void shouldRemoveAllOnDispose() {
		Scheduler scheduler = afterTest.autoDispose(Schedulers.newParallel("A", 2));

		Predicate<Meter.Id> meterPredicate = it -> {
			return scheduler.toString().equals(it.getTag(TAG_SCHEDULER_ID));
		};

		assertThat(registry.getMeters())
				.extracting(Meter::getId)
				.anyMatch(meterPredicate);

		scheduler.dispose();

		assertThat(registry.getMeters())
				.extracting(Meter::getId)
				.noneMatch(meterPredicate);
	}
}
