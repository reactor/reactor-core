package reactor.core.scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;

import reactor.core.Scannable;
import reactor.core.Scannable.Attr;

import static io.micrometer.core.instrument.Metrics.globalRegistry;

final class SchedulerMetricDecorator
			implements BiFunction<Scheduler, ScheduledExecutorService, ScheduledExecutorService> {


	final WeakHashMap<Scheduler, String>        SEEN_SCHEDULERS          = new WeakHashMap<>();
	final Map<String, AtomicInteger>            SCHEDULER_DIFFERENTIATOR = new HashMap<>();
	final WeakHashMap<Scheduler, AtomicInteger> EXECUTOR_DIFFERENTIATOR  = new WeakHashMap<>();

	@Override
	public synchronized ScheduledExecutorService apply(Scheduler scheduler, ScheduledExecutorService service) {
		//this is equivalent to `toString`, a detailed name like `parallel("foo", 3)`
		String schedulerDesc = Scannable
				.from(scheduler)
				.scanOrDefault(Attr.NAME, scheduler.getClass().getName());

		//we hope that each NAME is unique enough, but we'll differentiate by Scheduler
		String schedulerId =
				SEEN_SCHEDULERS.computeIfAbsent(scheduler, s -> {
					int schedulerDifferentiator = SCHEDULER_DIFFERENTIATOR
							.computeIfAbsent(schedulerDesc, k -> new AtomicInteger(0))
							.getAndIncrement();

					return (schedulerDifferentiator == 0) ? schedulerDesc
							: schedulerDesc + "#" + schedulerDifferentiator;
				});

		//we now want an executorId unique to a given scheduler
		String executorId = schedulerId + "-" +
				EXECUTOR_DIFFERENTIATOR.computeIfAbsent(scheduler, key -> new AtomicInteger(0))
						.getAndIncrement();


		// TODO return the result of ExecutorServiceMetrics#monitor
		//  once ScheduledExecutorService gets supported by Micrometer
		//  See https://github.com/micrometer-metrics/micrometer/issues/1021
		ExecutorServiceMetrics.monitor(globalRegistry, service, executorId,
				Tag.of("scheduler.id", schedulerId));

		return service;
	}
}
