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

	final Map<String, AtomicInteger>                                  SCHEDULERS_NAME_COUNTER = new HashMap<>();
	final Map<String, WeakHashMap<Scheduler, String>>                 UNIQUE_SCHEDULER_NAMES = new HashMap<>();

	final Map<String, AtomicInteger>                                  EXECUTORS_NAME_COUNTER = new HashMap<>();
	final Map<String, WeakHashMap<ScheduledExecutorService, String>>  EXECUTOR_LOCAL_IDS     = new HashMap<>();

	@Override
	public synchronized ScheduledExecutorService apply(Scheduler scheduler, ScheduledExecutorService service) {
		String schedulerName = Scannable
				.from(scheduler)
				.scanOrDefault(Attr.NAME, scheduler.getClass().getName());

		String schedulerId = UNIQUE_SCHEDULER_NAMES
				.computeIfAbsent(schedulerName, key -> new WeakHashMap<>())
				.computeIfAbsent(scheduler, key -> {
					int seqNum = SCHEDULERS_NAME_COUNTER
							.computeIfAbsent(schedulerName, nameKey -> new AtomicInteger())
	                        .getAndIncrement();

					if (seqNum > 0) {
						// If we got a name clash, append a sequential number to the name
						return schedulerName + "#" + seqNum;
					}
					else {
						return schedulerName;
					}
				});

		String executorName = EXECUTOR_LOCAL_IDS
				.computeIfAbsent(schedulerId, key -> new WeakHashMap<>())
				.computeIfAbsent(service, key -> {
					int seqNum = EXECUTORS_NAME_COUNTER
							.computeIfAbsent(schedulerId, nameKey -> new AtomicInteger())
                            .getAndIncrement();

					return schedulerId + "-" + seqNum;
				});

		// TODO return the result of ExecutorServiceMetrics#monitor
		//  once ScheduledExecutorService gets supported by Micrometer
		//  See https://github.com/micrometer-metrics/micrometer/issues/1021
		ExecutorServiceMetrics.monitor(globalRegistry, service, executorName,
				Tag.of("scheduler.id", schedulerId));

		return service;
	}
}
