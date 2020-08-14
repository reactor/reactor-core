package reactor.core.publisher;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks.Emission;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class VoidProcessor<T> extends MonoProcessor<T> implements Sinks.One<T> {

	volatile VoidInner<T>[] subscribers;

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<VoidProcessor, VoidInner[]> SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(VoidProcessor.class, VoidInner[].class, "subscribers");

	@SuppressWarnings("rawtypes")
	static final VoidInner[] EMPTY = new VoidInner[0];

	@SuppressWarnings("rawtypes")
	static final VoidInner[] TERMINATED = new VoidInner[0];

	Throwable error;

	VoidProcessor() {
		SUBSCRIBERS.lazySet(this, EMPTY);
	}

	@Override
	public Mono<T> asMono() {
		return this;
	}

	@Override
	public T peek() {
		if (!isTerminated()) {
			return null;
		}

		if (error != null) {
			RuntimeException re = Exceptions.propagate(error);
			re = Exceptions.addSuppressed(re, new Exception("VoidProcessor#peek cannot return a value since this mono has terminated with an error"));
			throw re;
		}

		return null;
	}

	@Override
	@Nullable
	public T block(@Nullable Duration timeout) {
		try {
			if (isTerminated()) {
				return peek();
			}

			long delay;
			if (null == timeout) {
				delay = 0L;
			}
			else {
				delay = System.nanoTime() + timeout.toNanos();
			}
			for (; ; ) {
				if (isTerminated()) {
					if (error != null) {
						RuntimeException re = Exceptions.propagate(error);
						re = Exceptions.addSuppressed(re, new Exception("Mono#block terminated with an error"));
						throw re;
					}
					return null;
				}
				if (timeout != null && delay < System.nanoTime()) {
					throw new IllegalStateException("Timeout on Mono blocking read, the operator is still waiting to observe a signal");
				}

				Thread.sleep(1);
			}

		}
		catch (InterruptedException ie) {
			Thread.currentThread()
				  .interrupt();

			throw new IllegalStateException("Thread Interruption on Mono blocking read");
		}
	}

	@Override
	public Emission emitEmpty() {
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
	public Emission emitError(Throwable cause) {
		Objects.requireNonNull(cause, "onError cannot be null");

		if (isTerminated()) {
			Operators.onErrorDroppedMulticast(cause, subscribers);
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
	@Deprecated
	public Emission emitValue(@Nullable T value) {
		if (value != null) {
			throw new UnsupportedOperationException("emitValue on VoidProcessor, which should be exposed as a Sinks.Empty");
		}
		return emitEmpty();
	}

	@Override
	public Context currentContext() {
		return Operators.multiSubscribersContext(subscribers);
	}

	@Override
	public long downstreamCount() {
		return subscribers.length;
	}

	@Override
	public void dispose() {
		if (isTerminated()) {
			return;
		}

		Exception e = new CancellationException("Disposed");
		error = e;

		for (VoidInner<?> as : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
			as.onError(e);
		}
	}

	@Override
	public final void onComplete() {
		emitEmpty();
	}

	@Override
	public final void onError(Throwable cause) {
		emitError(cause);
	}

	@Override
	public final void onNext(@Nullable T value) {
		if (value != null) {
			emitError(new UnsupportedOperationException("emitValue on VoidProcessor, which should be exposed as a Sinks.Empty"));
		}
		else {
			emitEmpty();
		}
	}

	@Override
	public final void onSubscribe(Subscription subscription) {
		subscription.request(Long.MAX_VALUE);
	}

	@Override
	public boolean isTerminated() {
		return SUBSCRIBERS.get(this) == TERMINATED;
	}

	@Override
	public Throwable getError() {
		return error;
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

	final static class VoidInner<T> extends AtomicBoolean implements InnerOperator<Void, T> {
		final VoidProcessor<T>          parent;
		final CoreSubscriber<? super T> actual;

		VoidInner(CoreSubscriber<? super T> actual, VoidProcessor<T> parent) {
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
