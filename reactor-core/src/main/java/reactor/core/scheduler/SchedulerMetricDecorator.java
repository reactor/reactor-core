package reactor.core.scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.micrometer.core.instrument.search.Search;

import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.Scannable.Attr;

import static io.micrometer.core.instrument.Metrics.globalRegistry;

final class SchedulerMetricDecorator
			implements BiFunction<Scheduler, ScheduledExecutorService, ScheduledExecutorService>,
			           Disposable {

	static final String TAG_SCHEDULER_ID = "reactor.scheduler.id";

	final WeakHashMap<Scheduler, String>        seenSchedulers          = new WeakHashMap<>();
	final Map<String, AtomicInteger>            schedulerDifferentiator = new HashMap<>();
	final WeakHashMap<Scheduler, AtomicInteger> executorDifferentiator  = new WeakHashMap<>();

	@Override
	public synchronized ScheduledExecutorService apply(Scheduler scheduler, ScheduledExecutorService service) {
		//this is equivalent to `toString`, a detailed name like `parallel("foo", 3)`
		String schedulerName = Scannable
				.from(scheduler)
				.scanOrDefault(Attr.NAME, scheduler.getClass().getName());

		//we hope that each NAME is unique enough, but we'll differentiate by Scheduler
		String schedulerId =
				seenSchedulers.computeIfAbsent(scheduler, s -> {
					int schedulerDifferentiator = this.schedulerDifferentiator
							.computeIfAbsent(schedulerName, k -> new AtomicInteger(0))
							.getAndIncrement();

					return (schedulerDifferentiator == 0) ? schedulerName
							: schedulerName + "#" + schedulerDifferentiator;
				});

		//we now want an executorId unique to a given scheduler
		String executorId = schedulerId + "-" +
				executorDifferentiator.computeIfAbsent(scheduler, key -> new AtomicInteger(0))
				                      .getAndIncrement();

		/*
		Design note: we assume that a given Scheduler won't apply the decorator twice to the
		same ExecutorService. Even though, it would simply create an extraneous meter for
		that ExecutorService, which we think is not that bad (compared to paying the price
		upfront of also tracking executors instances to deduplicate). The main goal is to
		detect Scheduler instances that have already started decorating their executors,
		in order to avoid consider two calls in a row as duplicates (yet still being able
		to distinguish between two instances with the same name and configuration).
		 */

		// TODO return the result of ExecutorServiceMetrics#monitor
		//  once ScheduledExecutorService gets supported by Micrometer
		//  See https://github.com/micrometer-metrics/micrometer/issues/1021
		ExecutorServiceMetrics.monitor(globalRegistry, service, executorId,
				Tag.of(TAG_SCHEDULER_ID, schedulerId));

		return service;
	}

	@Override
	public void dispose() {
		Search.in(globalRegistry)
		      .tagKeys(TAG_SCHEDULER_ID)
		      .meters()
		      .forEach(globalRegistry::remove);

		this.seenSchedulers.clear();
		this.schedulerDifferentiator.clear();
		this.executorDifferentiator.clear();
	}

	@Override
	public boolean isDisposed() {
		return false; //never really disposed
	}
}
