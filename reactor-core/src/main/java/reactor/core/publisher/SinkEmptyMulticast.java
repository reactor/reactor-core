package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks.Emission;
import reactor.util.context.Context;

final class SinkEmptyMulticast<T> extends Mono<T> implements Sinks.Empty<T>, ContextHolder {

	volatile VoidInner<T>[] subscribers;

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<SinkEmptyMulticast, VoidInner[]> SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(SinkEmptyMulticast.class, VoidInner[].class, "subscribers");

	@SuppressWarnings("rawtypes")
	static final VoidInner[] EMPTY = new VoidInner[0];

	@SuppressWarnings("rawtypes")
	static final VoidInner[] TERMINATED = new VoidInner[0];

	Throwable error;

	SinkEmptyMulticast() {
		SUBSCRIBERS.lazySet(this, EMPTY);
	}

	@Override
	public int currentSubscriberCount() {
		return subscribers.length;
	}

	@Override
	public Mono<T> asMono() {
		return this;
	}

	@Override
	public void emitEmpty() {
		//no particular error condition handling for onComplete
		@SuppressWarnings("unused")
		Emission emission = tryEmitEmpty();
	}

	@Override
	public Emission tryEmitEmpty() {
		VoidInner<?>[] array = SUBSCRIBERS.getAndSet(this, TERMINATED);

		if (array == TERMINATED) {
			return Emission.FAIL_TERMINATED;
		}

		for (VoidInner<?> as : array) {
			as.onComplete();
		}
		return Emission.OK;
	}

	@Override
	public void emitError(Throwable error) {
		Emission result = tryEmitError(error);
		if (result == Emission.FAIL_TERMINATED) {
			Operators.onErrorDroppedMulticast(error, subscribers);
		}
	}

	@Override
	public Emission tryEmitError(Throwable cause) {
		Objects.requireNonNull(cause, "onError cannot be null");

		if (subscribers == TERMINATED) {
			return Emission.FAIL_TERMINATED;
		}

		//guarded by a read memory barrier (isTerminated) and a subsequent write with getAndSet
		error = cause;

		for (VoidInner<?> as : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
			as.onError(cause);
		}
		return Emission.OK;
	}

	@Override
	public Context currentContext() {
		return Operators.multiSubscribersContext(subscribers);
	}

	boolean add(VoidInner<T> ps) {
		for (; ; ) {
			VoidInner<T>[] a = subscribers;

			if (a == TERMINATED) {
				return false;
			}

			int n = a.length;
			@SuppressWarnings("unchecked")
			VoidInner<T>[] b = new VoidInner[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = ps;

			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return true;
			}
		}
	}

	void remove(VoidInner<T> ps) {
		for (; ; ) {
			VoidInner<T>[] a = subscribers;
			int n = a.length;
			if (n == 0) {
				return;
			}

			int j = -1;
			for (int i = 0; i < n; i++) {
				if (a[i] == ps) {
					j = i;
					break;
				}
			}

			if (j < 0) {
				return;
			}

			VoidInner<?>[] b;

			if (n == 1) {
				b = EMPTY;
			}
			else {
				b = new VoidInner[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return;
			}
		}
	}

	@Override
	public void subscribe(final CoreSubscriber<? super T> actual) {
		VoidInner<T> as = new VoidInner<T>(actual, this);
		actual.onSubscribe(as);
		if (add(as)) {
			if (as.get()) {
				remove(as);
			}
		}
		else {
			if (as.get()) {
				return;
			}
			Throwable ex = error;
			if (ex != null) {
				actual.onError(ex);
			}
			else {
				actual.onComplete();
			}
		}
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Stream.of(subscribers);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.TERMINATED) return subscribers == TERMINATED;
		if (key == Attr.ERROR) return error;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	final static class VoidInner<T> extends AtomicBoolean implements InnerOperator<Void, T> {
		final SinkEmptyMulticast<T> parent;
		final CoreSubscriber<? super T> actual;

		VoidInner(CoreSubscriber<? super T> actual, SinkEmptyMulticast<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void cancel() {
			if (getAndSet(true)) {
				return;
			}

			parent.remove(this);
		}

		@Override
		public void request(long l) {
			Operators.validate(l);
		}

		@Override
		public void onSubscribe(Subscription s) {
			Objects.requireNonNull(s);
		}

		@Override
		public void onNext(Void aVoid) {

		}

		@Override
		public void onComplete() {
			if (get()) {
				return;
			}
			actual.onComplete();
		}

		@Override
		public void onError(Throwable t) {
			if (get()) {
				Operators.onOperatorError(t, currentContext());
				return;
			}
			actual.onError(t);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return parent;
			}
			if (key == Attr.CANCELLED) {
				return get();
			}
			if (key == Attr.RUN_STYLE) {
				return Attr.RunStyle.SYNC;
			}
			return InnerOperator.super.scanUnsafe(key);
		}
	}
}
