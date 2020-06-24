package reactor.core.publisher;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.util.stats.StatsMarker;
import reactor.util.stats.StatsReporter;

public class MonoStats<T> extends InternalMonoOperator<T, T> {

	final FluxOnAssembly.AssemblySnapshot assemblySnapshot;
	final StatsReporter                   statsReporter;
	final StatsMarker[]                   statsMarkers;

	protected MonoStats(Mono<? extends T> source,
			FluxOnAssembly.AssemblySnapshot assemblySnapshot,
			StatsReporter statsReporter,
			StatsMarker[] statsMarkers) {
		super(source);
		this.assemblySnapshot = assemblySnapshot;
		this.statsReporter = statsReporter;
		this.statsMarkers = statsMarkers;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new MonoStatsOperator<>(actual,
				assemblySnapshot,
				source,
				statsReporter,
				statsMarkers);
	}

	static class MonoStatsOperator<T> extends FluxStats.StatsOperator<T> {

		MonoStatsOperator(CoreSubscriber<? super T> actual,
				FluxOnAssembly.AssemblySnapshot snapshotStack,
				Publisher<?> parent,
				StatsReporter statsReporter,
				StatsMarker[] statsMarkers) {
			super(actual, snapshotStack, parent, statsReporter, statsMarkers);
		}

		@Override
		public void onNext(T t) {
			// specific mono case
			this.recordSignal(SignalType.ON_COMPLETE);
			this.detachIfInner();

			super.onNext(t);
		}
	}
}
