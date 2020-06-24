package reactor.core.publisher;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.stats.StatsMarker;
import reactor.util.stats.StatsReporter;

public class ParallelFluxStats<T> extends ParallelFlux<T> implements Scannable {

	final ParallelFlux<T>                 source;
	final FluxOnAssembly.AssemblySnapshot assemblySnapshot;
	final StatsReporter                   statsReporter;
	final StatsMarker[]                   statsMarkers;

	ParallelFluxStats(ParallelFlux<T> source,
			FluxOnAssembly.AssemblySnapshot assemblySnapshot,
			StatsReporter statsReporter,
			StatsMarker[] statsMarkers) {

		this.source = source;
		this.assemblySnapshot = assemblySnapshot;
		this.statsReporter = statsReporter;
		this.statsMarkers = statsMarkers;
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}

	@Override
	public int getPrefetch() {
		return source.getPrefetch();
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();

		return null;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void subscribe(CoreSubscriber<? super T>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}

		int n = subscribers.length;
		CoreSubscriber<? super T>[] parents = new CoreSubscriber[n];

		for (int i = 0; i < n; i++) {
			parents[i] = new ParallelStatsInner<>(subscribers[i],
					assemblySnapshot,
					source,
					statsReporter,
					statsMarkers);
		}

		source.subscribe(parents);
	}

	static class ParallelStatsInner<T> extends FluxStats.StatsOperator<T>
			implements InnerOperator<T, T>, Fuseable.QueueSubscription<T> {

		FluxStats.StatsOperator<?> serialDownstreamNode;

		boolean resolved;

		ParallelStatsInner(CoreSubscriber<? super T> actual,
				FluxOnAssembly.AssemblySnapshot snapshotStack,
				Publisher<?> parent,
				StatsReporter statsReporter,
				StatsMarker[] statsMarkers) {

			super(actual, snapshotStack, parent, statsReporter, statsMarkers);
		}

		@Override
		public void onNext(T t) {
			resolveChain();
			super.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			resolveChain();
			super.onError(t);
		}

		@Override
		public void onComplete() {
			resolveChain();
			super.onComplete();
		}

		@Override
		public void cancel() {
			resolveChain();
			super.cancel();
		}

		@Override
		public void attachTo(FluxStats.StatsOperator<?> downstreamNode) {
			if (downstreamNode instanceof ParallelStatsInner) {
				this.downstreamNode = downstreamNode;

				if (downstreamNode.upstreamNode != null) {
					downstreamNode.add(this);
				}
				else {
					downstreamNode.upstreamNode = this;
				}
				this.serialDownstreamNode = ((ParallelStatsInner<?>) downstreamNode).serialDownstreamNode;
			}
			else {
				this.isInner = true;
				this.serialDownstreamNode = downstreamNode;
			}
		}

		@Override
		public void detachIfInner() {
			// no-ops since parallel inner should not be detached
		}

		void resolveChain() {
			if (!resolved && !(this.upstreamNode instanceof ParallelStatsInner<?>)) {
				this.resolved = true;
				FluxStats.StatsOperator<?> upstreamNode =
						(FluxStats.StatsOperator<?>) Scannable.from(s)
						                                      .parents()
						                                      .filter(FluxStats.StatsOperator.class::isInstance)
						                                      .findFirst()
						                                      .orElse(null);
				ParallelStatsInner<?> tailNode = this.resolveTail();

				this.upstreamNode = null;
				this.serialDownstreamNode.upstreamNode = upstreamNode;

				tailNode.downstreamNode = upstreamNode;
				upstreamNode.downstreamNode = this.serialDownstreamNode;
				upstreamNode.add(tailNode);
			}
		}

		ParallelStatsInner<?> resolveTail() {
			if (this.downstreamNode instanceof ParallelFluxStats.ParallelStatsInner) {
				return ((ParallelStatsInner<?>) this.downstreamNode).resolveTail();
			}

			return this;
		}

	}

}
