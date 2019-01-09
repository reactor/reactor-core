package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicLong;

import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.util.annotation.Nullable;

public interface Tracker {

	boolean shouldCreateMarker();

	void onMarkerCreated(Marker marker);

	default Disposable onScopePassing(Marker marker) {
		return Disposables.disposed();
	}

	final class Marker {

		static final AtomicLong COUNTER = new AtomicLong();

		static final ThreadLocal<Marker> CURRENT = new ThreadLocal<>();

		@Nullable
		private final Marker parent;

		private final long id = COUNTER.getAndIncrement();

		Marker(@Nullable Marker parent) {
			this.parent = parent;
		}

		@Nullable
		public Marker getParent() {
			return parent;
		}

		@Override
		public String toString() {
			return "Marker{id=" + id + "}";
		}
	}

}
