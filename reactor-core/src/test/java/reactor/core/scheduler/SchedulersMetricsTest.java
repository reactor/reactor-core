package reactor.core.scheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import reactor.core.Disposable;
import reactor.core.Disposables;

import static org.assertj.core.api.Assertions.assertThat;

public class SchedulersMetricsTest {

	final SimpleMeterRegistry simpleMeterRegistry = new SimpleMeterRegistry();
	Disposable.Composite toCleanUp;

	@Before
	public void setUp() {
		Metrics.addRegistry(simpleMeterRegistry);
		Schedulers.enableMetrics();
		toCleanUp = Disposables.composite();
	}

	@After
	public void tearDown() {
		Schedulers.disableMetrics();
		Metrics.globalRegistry.forEachMeter(Metrics.globalRegistry::remove);
		Metrics.removeRegistry(simpleMeterRegistry);
		toCleanUp.dispose();
	}

	@Test
	public void metricsActivatedHasDistinctNameTags() {
		toCleanUp.add(Schedulers.newParallel("A", 3));
		toCleanUp.add(Schedulers.newParallel("B", 2));

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
		toCleanUp.add(Schedulers.newParallel("A", 4));
		toCleanUp.add(Schedulers.newParallel("A", 4));
		toCleanUp.add(Schedulers.newParallel("A", 3));
		toCleanUp.add(Schedulers.newSingle("B"));
		toCleanUp.add(Schedulers.newElastic("C").createWorker());

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
		toCleanUp.add(Schedulers.newParallel("A", 1));
		toCleanUp.add(Schedulers.newParallel("A", 1));
		toCleanUp.add(Schedulers.newParallel("A", 1));

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
	public void fromExecutorServiceSchedulerId() {
		toCleanUp.add(Schedulers.newParallel("foo", 3));
		toCleanUp.add(Schedulers.fromExecutorService(Executors.newSingleThreadScheduledExecutor()));
		toCleanUp.add(Schedulers.fromExecutorService(Executors.newSingleThreadScheduledExecutor()));
		toCleanUp.add(Schedulers.fromExecutorService(Executors.newSingleThreadScheduledExecutor(), "testService"));
		toCleanUp.add(Schedulers.fromExecutorService(Executors.newSingleThreadScheduledExecutor(), "testService"));

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
						"fromExecutorService(anonymous)-0",
						"fromExecutorService(anonymous)#1-0",
						"fromExecutorService(testService)-0",
						"fromExecutorService(testService)#1-0"
				);
	}

	@Test
	public void decorateTwiceWithSameSchedulerInstance() {
		Scheduler instance = Schedulers.newElastic("TWICE", 1);
		toCleanUp.add(instance);

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
		toCleanUp.add(Schedulers.newParallel("A", 1));
		toCleanUp.add(Schedulers.newParallel("A", 1));
		toCleanUp.add(Schedulers.newParallel("A", 1));

		Metrics.globalRegistry.counter("foo", "tagged", "bar");

		Schedulers.disableMetrics();

		assertThat(simpleMeterRegistry.getMeters()
		                              .stream()
		                              .map(m -> m.getId().getName())
		                              .distinct())
				.containsExactly("foo");
	}
}
