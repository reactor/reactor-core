package reactor.core.publisher;

import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.stats.StatsMarker;
import reactor.util.stats.StatsReporter;

public class ConnectableFluxStats<T> extends InternalConnectableFluxOperator<T, T> {

	final FluxOnAssembly.AssemblySnapshot assemblySnapshot;
	final StatsReporter                   statsReporter;
	final StatsMarker[]                   statsMarkers;

	protected ConnectableFluxStats(ConnectableFlux<T> source,
			FluxOnAssembly.AssemblySnapshot assemblySnapshot,
			StatsReporter statsReporter,
			StatsMarker[] statsMarkers) {
		super(source);
		this.assemblySnapshot = assemblySnapshot;
		this.statsReporter = statsReporter;
		this.statsMarkers = statsMarkers;
	}

	@Override
	public void connect(Consumer<? super Disposable> cancelSupport) {
		source.connect(cancelSupport);
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {

		if (nextOptimizableSource() == null) {
			return new ConnectableStatsInner<>(actual,
					assemblySnapshot,
					source,
					statsReporter,
					statsMarkers);
		}
		else {
			return new FluxStats.StatsOperator<>(actual,
					assemblySnapshot,
					source,
					statsReporter,
					statsMarkers);
		}
	}

	static class ConnectableStatsInner<T> extends FluxStats.StatsOperator<T>
			implements InnerOperator<T, T>, Fuseable.QueueSubscription<T> {

		ConnectableStatsInner(CoreSubscriber<? super T> actual,
				FluxOnAssembly.AssemblySnapshot snapshotStack,
				Publisher<?> parent,
				StatsReporter statsReporter,
				StatsMarker[] statsMarkers) {

			super(actual, snapshotStack, parent, statsReporter, statsMarkers);
		}

		@Override
		public void onNext(T t) {
			resolveUpstream();
			super.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			resolveUpstream();
			super.onError(t);
		}

		@Override
		public void onComplete() {
			resolveUpstream();
			super.onComplete();
		}

		@Override
		public void cancel() {
			resolveUpstream();
			FluxStats.StatsOperator<?> connectableStatsOperator = this.upstreamNode;
			if (connectableStatsOperator instanceof FluxStats.MultiConsumersStatsOperator) {
				((FluxStats.MultiConsumersStatsOperator<?>) connectableStatsOperator).disconnect();
			}
			super.cancel();
		}

		void resolveUpstream() {
			if (this.upstreamNode == null) {
				FluxStats.MultiConsumersStatsOperator<?>
						multiConsumersStatsOperator = (FluxStats.MultiConsumersStatsOperator<?>) Scannable.from(s)
						                                                                                  .parents()
						                                                                                  .filter(FluxStats.MultiConsumersStatsOperator.class::isInstance)
						                                                                                  .findFirst()
						                                                                                  .orElse(null);

				this.upstreamNode = multiConsumersStatsOperator;
				multiConsumersStatsOperator.connect();
			}
		}
	}
}
