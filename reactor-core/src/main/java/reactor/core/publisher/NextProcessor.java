package reactor.core.publisher;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

// NextProcessor extends a deprecated class but is itself not deprecated and is here to stay, hence the following line is ok.
@SuppressWarnings("deprecation")
class NextProcessor<O> extends MonoProcessor<O> implements InternalOneSink<O> {

	volatile NextInner<O>[] subscribers;

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<NextProcessor, NextInner[]> SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(NextProcessor.class, NextInner[].class, "subscribers");

	@SuppressWarnings("rawtypes")
	static final NextInner[] EMPTY = new NextInner[0];

	@SuppressWarnings("rawtypes")
	static final NextInner[] TERMINATED = new NextInner[0];

	@SuppressWarnings("rawtypes")
	static final NextInner[] EMPTY_WITH_SOURCE = new NextInner[0];

	volatile     Subscription                                             subscription;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<NextProcessor, Subscription> UPSTREAM =
			AtomicReferenceFieldUpdater.newUpdater(NextProcessor.class, Subscription.class, "subscription");

	@Nullable
	CorePublisher<? extends O> source;
	@Nullable
	Throwable error;
	@Nullable
	O         value;

	NextProcessor(@Nullable CorePublisher<? extends O> source) {
		this.source = source;
		SUBSCRIBERS.lazySet(this, source != null ? EMPTY_WITH_SOURCE : EMPTY);
	}

	@Override
	public int currentSubscriberCount() {
		return subscribers.length;
	}

	@Override
	public Mono<O> asMono() {
		return this;
	}

	@Override
	public O peek() {
		if (!isTerminated()) {
			return null;
		}

		if (value != null) {
			return value;
		}

		if (error != null) {
			RuntimeException re = Exceptions.propagate(error);
			re = Exceptions.addSuppressed(re, new Exception("Mono#peek terminated with an error"));
			throw re;
		}

		return null;
	}

	@Override
	@Nullable
	public O block(@Nullable Duration timeout) {
		try {
			if (isTerminated()) {
				return peek();
			}

			connect();

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
					return value;
				}
				if (timeout != null && delay < System.nanoTime()) {
					cancel();
					throw new IllegalStateException("Timeout on Mono blocking read");
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
	public final void onComplete() {
		//no particular error condition handling for onComplete
		@SuppressWarnings("unused") EmitResult emitResult = tryEmitEmpty();
	}

	@Override
	public EmitResult tryEmitEmpty() {
		return tryEmitValue(null);
	}

	@Override
	public final void onError(Throwable cause) {
		emitError(cause, EmitFailureHandler.FAIL_FAST);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Sinks.EmitResult tryEmitError(Throwable cause) {
		Objects.requireNonNull(cause, "onError cannot be null");

		if (UPSTREAM.getAndSet(this, Operators.cancelledSubscription()) == Operators.cancelledSubscription()) {
			return EmitResult.FAIL_TERMINATED;
		}

		error = cause;
		value = null;
		source = null;

		//no need to double check since UPSTREAM.getAndSet gates the completion already
		for (NextInner<O> as : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
			as.onError(cause);
		}
		return EmitResult.OK;
	}

	@Override
	public final void onNext(@Nullable O value) {
		emitValue(value, EmitFailureHandler.FAIL_FAST);
	}

	@Override
	public EmitResult tryEmitValue(@Nullable O value) {
		Subscription s;
		if ((s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription())) == Operators.cancelledSubscription()) {
			return EmitResult.FAIL_TERMINATED;
		}

		this.value = value;
		Publisher<? extends O> parent = source;
		source = null;

		@SuppressWarnings("unchecked") NextInner<O>[] array = SUBSCRIBERS.getAndSet(this, TERMINATED);

		if (value == null) {
			for (NextInner<O> as : array) {
				as.onComplete();
			}
		}
		else {
			if (s != null && !(parent instanceof Mono)) {
				s.cancel();
			}

			for (NextInner<O> as : array) {
				as.complete(value);
			}
		}
		return EmitResult.OK;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT)
			return subscription;
		return super.scanUnsafe(key);
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
	@SuppressWarnings("unchecked")
	public void dispose() {
		Subscription s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription());
		if (s == Operators.cancelledSubscription()) {
			return;
		}

		source = null;
		if (s != null) {
			s.cancel();
		}


		NextInner<O>[] a;
		if ((a = SUBSCRIBERS.getAndSet(this, TERMINATED)) != TERMINATED) {
			Exception e = new CancellationException("Disposed");
			error = e;
			value = null;

			for (NextInner<O> as : a) {
				as.onError(e);
			}
		}
	}

	@Override
	// This method is inherited from a deprecated class and will be removed in 3.5.
	@SuppressWarnings("deprecation")
	public void cancel() {
		if (isTerminated()) {
			return;
		}

		Subscription s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription());
		if (s == Operators.cancelledSubscription()) {
			return;
		}

		source = null;
		if (s != null) {
			s.cancel();
		}
	}

	@Override
	public final void onSubscribe(Subscription subscription) {
		if (Operators.setOnce(UPSTREAM, this, subscription)) {
			subscription.request(Long.MAX_VALUE);
		}
	}

	@Override
	// This method is inherited from a deprecated class and will be removed in 3.5.
	@SuppressWarnings("deprecation")
	public boolean isCancelled() {
		return subscription == Operators.cancelledSubscription() && !isTerminated();
	}

	@Override
	public boolean isTerminated() {
		return subscribers == TERMINATED;
	}

	@Nullable
	@Override
	public Throwable getError() {
		return error;
	}

	boolean add(NextInner<O> ps) {
		for (; ; ) {
			NextInner<O>[] a = subscribers;

			if (a == TERMINATED) {
				return false;
			}

			int n = a.length;
			@SuppressWarnings("unchecked") NextInner<O>[] b = new NextInner[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = ps;

			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				Publisher<? extends O> parent = source;
				if (parent != null && a == EMPTY_WITH_SOURCE) {
					parent.subscribe(this);
				}
				return true;
			}
		}
	}

	@SuppressWarnings("unchecked")
	void remove(NextInner<O> ps) {
		for (; ; ) {
			NextInner<O>[] a = subscribers;
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

			NextInner<O>[] b;

			if (n == 1) {
				b = EMPTY;
			}
			else {
				b = new NextInner[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return;
			}
		}
	}

	@Override
	public void subscribe(final CoreSubscriber<? super O> actual) {
		NextInner<O> as = new NextInner<>(actual, this);
		actual.onSubscribe(as);
		if (add(as)) {
			if (as.isCancelled()) {
				remove(as);
			}
		}
		else {
			Throwable ex = error;
			if (ex != null) {
				actual.onError(ex);
			}
			else {
				O v = value;
				if (v != null) {
					as.complete(v);
				}
				else {
					as.onComplete();
				}
			}
		}
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Stream.of(subscribers);
	}

	void connect() {
		Publisher<? extends O> parent = source;
		if (parent != null && SUBSCRIBERS.compareAndSet(this, EMPTY_WITH_SOURCE, EMPTY)) {
			parent.subscribe(this);
		}
	}

	final static class NextInner<T> extends Operators.MonoSubscriber<T, T> {
		final NextProcessor<T> parent;

		NextInner(CoreSubscriber<? super T> actual, NextProcessor<T> parent) {
			super(actual);
			this.parent = parent;
		}

		@Override
		public void cancel() {
			if (STATE.getAndSet(this, CANCELLED) != CANCELLED) {
				parent.remove(this);
			}
		}

		@Override
		public void onComplete() {
			if (!isCancelled()) {
				actual.onComplete();
			}
		}

		@Override
		public void onError(Throwable t) {
			if (!isCancelled()) {
				actual.onError(t);
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return parent;
			}
			if (key == Attr.RUN_STYLE) {
				return Attr.RunStyle.SYNC;
			}
			return super.scanUnsafe(key);
		}
	}
}
