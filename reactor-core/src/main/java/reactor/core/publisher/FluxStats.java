package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.stats.StatsMarker;
import reactor.util.stats.StatsNode;
import reactor.util.stats.StatsReporter;

import static reactor.core.Fuseable.SYNC;

public class FluxStats<T> extends InternalFluxOperator<T, T> {

	final FluxOnAssembly.AssemblySnapshot assemblySnapshot;
	final StatsReporter        statsReporter;
	final StatsMarker[]        statsMarkers;

	protected FluxStats(Flux<? extends T> source,
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
		return wrapSubscriber(actual,
				source,
				assemblySnapshot,
				statsReporter,
				statsMarkers);
	}


	static <T> CoreSubscriber<? super T> wrapSubscriber(CoreSubscriber<? super T> actual,
			Flux<? extends T> source,
			FluxOnAssembly.AssemblySnapshot snapshotStack,
			StatsReporter statsReporter,
			StatsMarker[] statsMarkers) {

		if (actual instanceof FluxPublish.PublishSubscriber) {
			return new MultiConsumersStatsOperator<>(actual,
					snapshotStack,
					source,
					statsReporter,
					statsMarkers
			);
		}
		else {
			return new StatsOperator<>(actual,
					snapshotStack,
					source,
					statsReporter,
					statsMarkers
			);
		}
	}

	static class StatsOperator<T> extends StatsNode<StatsOperator<?>>
			implements InnerOperator<T, T>, Fuseable.QueueSubscription<T> {
		@SuppressWarnings("rawtypes")
		static final Class<StatsOperator> KEY = StatsOperator.class;

		final FluxOnAssembly.AssemblySnapshot snapshotStack;
		final Publisher<?>                    parent;
		final CoreSubscriber<? super T>       actual;
		final Context                         currentContext;
		final boolean                         reportOnTerminal;

		final StatsReporter statsReporter;
		final StatsMarker[] statsMarkers;

		Fuseable.QueueSubscription<T> qs;
		Subscription                  s;
		int                           fusionMode;

		StatsOperator(CoreSubscriber<? super T> actual,
				FluxOnAssembly.AssemblySnapshot snapshotStack,
				Publisher<?> parent,
				StatsReporter statsReporter,
				StatsMarker[] statsMarkers) {

			this.snapshotStack = snapshotStack;
			this.parent = parent;
			this.actual = actual;
			this.statsReporter = statsReporter;
			this.statsMarkers = statsMarkers;

			Context context = actual.currentContext();
			StatsOperator<?> downstreamStatsOperator = context.getOrDefault(KEY, null);

			if (downstreamStatsOperator != null) {
				this.reportOnTerminal = false;
				this.attachTo(downstreamStatsOperator);
			}
			else {
				this.reportOnTerminal = true;
			}

			this.currentContext = context.put(KEY, this);
		}

		@Override
		public void attachTo(StatsOperator<?> downstreamNode) {
			Publisher<?> parent = downstreamNode.parent;
			if ((!(parent instanceof OptimizableOperator) || ((OptimizableOperator<?,
					?>) parent).nextOptimizableSource() == null) && !(parent instanceof MonoIgnoreThen || parent instanceof ConnectableFlux || parent instanceof ParallelSource || this.parent instanceof ConnectableFlux)) {
				this.downstreamNode = downstreamNode;
				this.isInner = true;
				downstreamNode.add(this);
				return;
			}

			super.attachTo(downstreamNode);
		}

		@Override
		public final CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.ACTUAL_METADATA) return !snapshotStack.checkpointed;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				this.qs = Operators.as(s);

				this.recordSignal(SignalType.ON_SUBSCRIBE);

				this.actual.onSubscribe(this);
			}
		}

		@Override
		final public int requestFusion(int requestedMode) {
			Fuseable.QueueSubscription<T> qs = this.qs;
			if (qs != null) {
				int m = qs.requestFusion(requestedMode);
				if (m != Fuseable.NONE) {
					this.fusionMode = m;
					if (m == SYNC) {
						recordRequest(Long.MAX_VALUE);
					}
				}
				return m;
			}
			return Fuseable.NONE;
		}

		@Override
		public void onNext(T t) {

			this.recordProduced();
			this.recordSignal(SignalType.ON_NEXT);

			this.actual.onNext(t);

			// workaround to measure of the source publisher speed rather than source
			// and the whole pipeline
			if (this.upstreamNode == null) {
				this.lastElapsedOnNextNanos = System.nanoTime();
			}
		}

		@Override
		public void onError(Throwable t) {
			this.error = t;
			this.recordSignal(SignalType.ON_ERROR);

			if (this.reportOnTerminal()) {
				this.statsReporter.report(this, statsMarkers);
			}

			this.actual.onError(t);
		}

		@Override
		public void onComplete() {
			this.recordSignal(SignalType.ON_COMPLETE);

			if (this.reportOnTerminal()) {
				this.statsReporter.report(this, statsMarkers);
			}

			this.detachIfInner();

			this.actual.onComplete();
		}

		@Override
		public void request(long n) {
			this.recordSignal(SignalType.REQUEST);
			this.recordRequest(n);

			this.s.request(n);
		}

		@Override
		public void cancel() {
			this.recordSignal(SignalType.CANCEL);

			if (this.reportOnTerminal()) {
				this.statsReporter.report(this, this.statsMarkers);
			}

			this.s.cancel();
		}

		boolean reportOnTerminal() {
			return reportOnTerminal;
		}

		@Override
		final public Context currentContext() {
			return this.currentContext;
		}

		@Override
		final public int size() {
			return qs.size();
		}

		@Override
		final public boolean isEmpty() {
			try {
				return qs.isEmpty();
			}
			catch (Throwable ex) {
				Exceptions.throwIfFatal(ex);
				throw Exceptions.propagate(ex);
			}
		}

		@Override
		final public void clear() {
			qs.clear();
		}

		@Override
		@Nullable
		final public T poll() {
			try {
				T element = qs.poll();

				if (element != null) {
					this.recordProduced();
					this.recordSignal(SignalType.ON_NEXT);
				} else {
					if (this.fusionMode == SYNC) {
						this.done = true;

						this.recordSignal(SignalType.ON_COMPLETE);

						if (this.reportOnTerminal) {
							this.statsReporter.report(this, statsMarkers);
						}
					}
				}

				return element;
			}
			catch (final Throwable ex) {
				this.error = ex;
				this.done = true;

				this.recordSignal(SignalType.ON_ERROR);

				if (this.reportOnTerminal) {
					this.statsReporter.report(this, statsMarkers);
				}

				Exceptions.throwIfFatal(ex);
				throw Exceptions.propagate(ex);
			}
		}

		@Override
		final public String toString() {
			return this.snapshotStack.operatorAssemblyInformation();
		}

		@Override
		final public String stepName() {
			return toString();
		}

		String[] cachedParts;
		@Override
		public String operatorName() {
			String[] parts = cachedParts;

			if (parts == null) {
				parts = Traces.extractOperatorAssemblyInformationParts(this.snapshotStack.toAssemblyInformation());
				cachedParts = parts;
			}

			return parts.length > 1 ? parts[0] : "";
		}

		@Override
		public String line() {
			String[] parts = cachedParts;

			if (parts == null) {
				parts = Traces.extractOperatorAssemblyInformationParts(this.snapshotStack.toAssemblyInformation());
				cachedParts = parts;
			}

			return parts[parts.length - 1];
		}
	}

	static class MultiConsumersStatsOperator<T> extends StatsOperator<T>
			implements InnerOperator<T, T>, Fuseable.QueueSubscription<T> {

		volatile int consumers;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MultiConsumersStatsOperator> CONSUMERS =
				AtomicIntegerFieldUpdater.newUpdater(MultiConsumersStatsOperator.class, "consumers");

		MultiConsumersStatsOperator(CoreSubscriber<? super T> actual,
				FluxOnAssembly.AssemblySnapshot snapshotStack,
				Publisher<?> parent,
				StatsReporter statsReporter,
				StatsMarker[] statsMarkers) {

			super(actual, snapshotStack, parent, statsReporter, statsMarkers);
		}

		@Override
		public void attachTo(StatsOperator<?> downstreamNode) {
			// do nothing here
		}

		void connect() {
			CONSUMERS.getAndIncrement(this);
		}


		void disconnect() {
			CONSUMERS.decrementAndGet(this);
		}

		@Override
		boolean reportOnTerminal() {
			return this.consumers == 0;
		}
	}
}
