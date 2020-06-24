package reactor.util.stats;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.publisher.SignalType;
import reactor.util.annotation.Nullable;

public abstract class StatsNode<SELF extends StatsNode<SELF>> {

	@Nullable
	public SELF upstreamNode;
	@Nullable
	public SELF downstreamNode;

	static final StatsNode<?>[] EMPTY = new StatsNode[0];

	public volatile SELF[] innerNodes;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<StatsNode, StatsNode[]> INNER_NODES
		= AtomicReferenceFieldUpdater.newUpdater(StatsNode.class, StatsNode[].class, "innerNodes");

	public SignalType lastObservedSignal;

	public long lastElapsedOnNextNanos;

	public long totalSumProcessingTimeInNanos;

	public long produced;
	public long requested;

	public Throwable error;
	public boolean   done;
	public boolean   cancelled;

	public boolean isInner;

	protected StatsNode() {
		INNER_NODES.lazySet(this, EMPTY);
	}

	public void recordSignal(SignalType signalType) {
		long currentNanoTime;

		switch (signalType) {
			case ON_SUBSCRIBE:
				this.lastObservedSignal = signalType;
				break;
			case ON_NEXT:
				currentNanoTime = System.nanoTime();
				if (this.upstreamNode == null) {
					this.totalSumProcessingTimeInNanos += (currentNanoTime - this.lastElapsedOnNextNanos);
				}
				else {
					this.totalSumProcessingTimeInNanos += (currentNanoTime - this.upstreamNode.lastElapsedOnNextNanos);
				}
				this.lastElapsedOnNextNanos = currentNanoTime;
				this.lastObservedSignal = signalType;
				break;
			case REQUEST:
				currentNanoTime = System.nanoTime();
				if (this.produced == this.requested) {
					this.lastElapsedOnNextNanos = currentNanoTime;
				}
				this.lastObservedSignal = signalType;
				break;
			case CANCEL:
				this.cancelled = true;
				break;
			case ON_ERROR:
			case ON_COMPLETE:
				this.done = true;
				break;
		}
	}

	public void recordRequest(long requested) {
		this.requested += requested;
	}

	public void recordProduced() {
		this.produced++;
	}

	public void attachTo(SELF downstreamNode) {
		this.downstreamNode = downstreamNode;

		if (downstreamNode.upstreamNode != null) {
			this.isInner = true;
			downstreamNode.add(this);
		}
		else {
			//noinspection unchecked
			downstreamNode.upstreamNode = (SELF) this;
		}
	}

	public void detachIfInner() {
		if (this.isInner) {
			//noinspection ConstantConditions
			this.downstreamNode.remove(this);
		}
	}

	public void add(StatsNode<?> uip) {
		for (;;) {
			StatsNode<?>[] a = innerNodes;

			int n = a.length;
			StatsNode<?>[] b = new StatsNode[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = uip;

			if (INNER_NODES.compareAndSet(this, a, b)) {
				return;
			}
		}
	}

	public void remove(StatsNode<?> uip) {
		for (;;) {
			StatsNode<?>[] a = innerNodes;
			int n = a.length;
			if (n == 0) {
				return;
			}

			int j = -1;
			for (int i = 0; i < n; i++) {
				if (a[i] == uip) {
					j = i;
					break;
				}
			}

			if (j < 0) {
				return;
			}

			StatsNode<?>[] b;

			if (n == 1) {
				b = EMPTY;
			} else {
				b = new StatsNode[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (INNER_NODES.compareAndSet(this, a, b)) {
				return;
			}
		}
	}

	public abstract String operatorName();

	public abstract String line();

}