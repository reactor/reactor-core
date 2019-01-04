package reactor.core.publisher;

import reactor.core.Disposable;
import reactor.core.Disposables;

public interface Tracer {

	boolean shouldTrace();

	void onSpanCreated(Span span);

	default Disposable onScopePassing(Span span) {
		return Disposables.disposed();
	}

	final class Span {

		static final ThreadLocal<Span> CURRENT = new ThreadLocal<>();

		Span() {
		}
	}

}
