package reactor.core.publisher;

import java.util.Objects;
import java.util.function.Consumer;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;

class ConnectableFluxRestoringThreadLocals<T> extends ConnectableFlux<T> {

	private final ConnectableFlux<T> source;

	public ConnectableFluxRestoringThreadLocals(ConnectableFlux<T> source) {
		this.source = Objects.requireNonNull(source, "source");
	}

	@Override
	public void connect(Consumer<? super Disposable> cancelSupport) {
		source.connect(cancelSupport);
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		source.subscribe(Operators.restoreContextOnSubscriber(source, actual));
	}
}
