package reactor.core.publisher;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;

public class ParallelFluxRestoringThreadLocals<T> extends ParallelFlux<T> implements
                                                                          Scannable {

	private final ParallelFlux<? extends T> source;

	public ParallelFluxRestoringThreadLocals(ParallelFlux<? extends T> source) {
		this.source = source;
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}

	@Override
	public void subscribe(CoreSubscriber<? super T>[] subscribers) {
		CoreSubscriber<? super T>[] actualSubscribers =
				Operators.restoreContextOnSubscribersIfNecessary(source, subscribers);

		source.subscribe(actualSubscribers);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		if (key == Attr.INTERNAL_PRODUCER) return true;
		return null;
	}
}
