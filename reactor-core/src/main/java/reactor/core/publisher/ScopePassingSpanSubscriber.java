package reactor.core.publisher;

import java.util.Collection;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Scannable;
import reactor.util.context.Context;

final class ScopePassingSpanSubscriber<T>
		implements CoreSubscriber<T>, Subscription, Scannable {

	private final Subscriber<? super T> subscriber;

	private final Context context;

	private final Collection<Tracer> tracers;

	private final Tracer.Span span;

	private Subscription s;

	ScopePassingSpanSubscriber(Subscriber<? super T> subscriber,
			Context ctx,
			Collection<Tracer> tracers,
			Tracer.Span span) {
		this.subscriber = subscriber;
		this.context = ctx;
		this.tracers = tracers;
		this.span = span;
	}

	@Override
	public void onSubscribe(Subscription subscription) {
		this.s = subscription;

		Tracer.Span.CURRENT.set(span);
		try {
			Disposable.Composite contextDisposables = Disposables.composite();

			for (Tracer tracer : tracers) {
				contextDisposables.add(tracer.onScopePassing(span));
			}

			try {
				this.subscriber.onSubscribe(this);
			}
			finally {
				contextDisposables.dispose();
			}
		}
		finally {
			Tracer.Span.CURRENT.remove();
		}
	}

	@Override
	public void request(long n) {
		Tracer.Span.CURRENT.set(span);
		try {
			Disposable.Composite contextDisposables = Disposables.composite();

			for (Tracer tracer : tracers) {
				contextDisposables.add(tracer.onScopePassing(span));
			}

			try {
				this.s.request(n);
			}
			finally {
				contextDisposables.dispose();
			}
		}
		finally {
			Tracer.Span.CURRENT.remove();
		}
	}

	@Override
	public void cancel() {
		Tracer.Span.CURRENT.set(span);
		try {
			Disposable.Composite contextDisposables = Disposables.composite();

			for (Tracer tracer : tracers) {
				contextDisposables.add(tracer.onScopePassing(span));
			}

			try {
				this.s.cancel();
			}
			finally {
				contextDisposables.dispose();
			}
		}
		finally {
			Tracer.Span.CURRENT.remove();
		}
	}

	@Override
	public void onNext(T o) {
		Tracer.Span.CURRENT.set(span);
		try {
			Disposable.Composite contextDisposables = Disposables.composite();

			for (Tracer tracer : tracers) {
				contextDisposables.add(tracer.onScopePassing(span));
			}

			try {
				this.subscriber.onNext(o);
			}
			finally {
				contextDisposables.dispose();
			}
		}
		finally {
			Tracer.Span.CURRENT.remove();
		}
	}

	@Override
	public void onError(Throwable throwable) {
		Tracer.Span.CURRENT.set(span);
		try {
			Disposable.Composite contextDisposables = Disposables.composite();

			for (Tracer tracer : tracers) {
				contextDisposables.add(tracer.onScopePassing(span));
			}

			try {
				this.subscriber.onError(throwable);
			}
			finally {
				contextDisposables.dispose();
			}
		}
		finally {
			Tracer.Span.CURRENT.remove();
		}
	}

	@Override
	public void onComplete() {
		Tracer.Span.CURRENT.set(span);
		try {
			Disposable.Composite contextDisposables = Disposables.composite();

			for (Tracer tracer : tracers) {
				contextDisposables.add(tracer.onScopePassing(span));
			}

			try {
				this.subscriber.onComplete();
			}
			finally {
				contextDisposables.dispose();
			}
		}
		finally {
			Tracer.Span.CURRENT.remove();
		}
	}

	@Override
	public Context currentContext() {
		return this.context;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return this.s;
		if (key == Attr.ACTUAL) return this.subscriber;
		return null;
	}

}
