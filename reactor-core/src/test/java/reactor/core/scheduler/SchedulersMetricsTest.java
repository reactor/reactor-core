package reactor.core.scheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

public class SchedulersMetricsTest {

	final SimpleMeterRegistry simpleMeterRegistry = new SimpleMeterRegistry();

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
	public void testUsesCustomizedDefaultRegistry() {
		Schedulers.disableMetrics();
		SimpleMeterRegistry otherRegistry = new SimpleMeterRegistry();
		reactor.util.Metrics.setUnsafeRegistry(otherRegistry);
		Schedulers.enableMetrics();

		try {
			assertThat(otherRegistry.getMeters()).isEmpty();

			Schedulers.newParallel("A", 1);

			Metrics.globalRegistry.getMeters().stream().map(m -> m.getId().getName())
					.forEach(System.out::println);

			assertThat(otherRegistry.getMeters()).as("registered meters on default registry").isNotEmpty();
		}
		finally {
			reactor.util.Metrics.setUnsafeRegistry(null);
			otherRegistry.close();
		}
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
		                              .map(m -> m.getId().getTag(SchedulerMetricDecorator.TAG_SCHEDULER_ID))
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
}
