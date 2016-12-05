package reactor.test.utils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.MultiProducer;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Operators;

/**
 * A {@link Processor} derived from {@link reactor.core.publisher.DirectProcessor} except
 * it allows {@link #onNext(Object)} calls even when the requested counter is &lt;= 0.
 *
 * @author Simon Baslé
 */
public final class RequestIgnoringProcessor<T>
		extends FluxProcessor<T, T>
		implements Receiver, MultiProducer {


	/**
	 * Create a new {@link RequestIgnoringProcessor}
	 * @param <E> Type of processed signals
	 * @return a fresh processor
	 */
	public static <E> RequestIgnoringProcessor<E> create() {
		return new RequestIgnoringProcessor<>();
	}

	@SuppressWarnings("rawtypes")
	private static final RequestIgnoringSubscription[] EMPTY = new RequestIgnoringSubscription[0];

	@SuppressWarnings("rawtypes")
	private static final RequestIgnoringSubscription[] TERMINATED = new RequestIgnoringSubscription[0];

	@SuppressWarnings("unchecked")
	private volatile     RequestIgnoringSubscription<T>[] subscribers = EMPTY;
	@SuppressWarnings("rawtypes")
	private static final AtomicReferenceFieldUpdater<RequestIgnoringProcessor, RequestIgnoringSubscription[]>
	                                                      SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(RequestIgnoringProcessor.class,
					RequestIgnoringSubscription[].class,
					"subscribers");

	Throwable error;

	RequestIgnoringProcessor() {
	}

	@Override
	public long getPrefetch() {
		return Long.MAX_VALUE;
	}

	@Override
	public void onSubscribe(Subscription s) {
		Objects.requireNonNull(s, "s");
		if (subscribers != TERMINATED) {
			s.request(Long.MAX_VALUE);
		} else {
			s.cancel();
		}
	}

	@Override
	public void onNext(T t) {
		Objects.requireNonNull(t, "t");

		for (RequestIgnoringSubscription<T> s : subscribers) {
			s.onNext(t);
		}
	}

	@Override
	public void onError(Throwable t) {
		Objects.requireNonNull(t, "t");

		error = t;
		for (RequestIgnoringSubscription<?> s : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
			s.onError(t);
		}
	}

	@Override
	public void onComplete() {
		for (RequestIgnoringSubscription<?> s : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
			s.onComplete();
		}
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		Objects.requireNonNull(s, "s");

		RequestIgnoringSubscription<T>
				p = new RequestIgnoringSubscription<>(s, this);
		s.onSubscribe(p);

		if (add(p)) {
			if (p.cancelled) {
				remove(p);
			}
		} else {
			Throwable e = error;
			if (e != null) {
				s.onError(e);
			} else {
				s.onComplete();
			}
		}
	}

	@Override
	public boolean isStarted() {
		return true;
	}

	@Override
	public boolean isTerminated() {
		return TERMINATED == subscribers;
	}

	@Override
	public Iterator<?> downstreams() {
		return Arrays.asList(subscribers).iterator();
	}

	@Override
	public long downstreamCount() {
		return subscribers.length;
	}

	boolean add(RequestIgnoringSubscription<T> s) {
		RequestIgnoringSubscription<T>[] a = subscribers;
		if (a == TERMINATED) {
			return false;
		}

		synchronized (this) {
			a = subscribers;
			if (a == TERMINATED) {
				return false;
			}
			int len = a.length;

			@SuppressWarnings("unchecked") RequestIgnoringSubscription<T>[] b = new RequestIgnoringSubscription[len + 1];
			System.arraycopy(a, 0, b, 0, len);
			b[len] = s;

			subscribers = b;

			return true;
		}
	}

	@SuppressWarnings("unchecked")
	void remove(RequestIgnoringSubscription<T> s) {
		RequestIgnoringSubscription<T>[] a = subscribers;
		if (a == TERMINATED || a == EMPTY) {
			return;
		}

		synchronized (this) {
			a = subscribers;
			if (a == TERMINATED || a == EMPTY) {
				return;
			}
			int len = a.length;

			int j = -1;

			for (int i = 0; i < len; i++) {
				if (a[i] == s) {
					j = i;
					break;
				}
			}
			if (j < 0) {
				return;
			}
			if (len == 1) {
				subscribers = EMPTY;
				return;
			}

			RequestIgnoringSubscription<T>[] b = new RequestIgnoringSubscription[len - 1];
			System.arraycopy(a, 0, b, 0, j);
			System.arraycopy(a, j + 1, b, j, len - j - 1);

			subscribers = b;
		}
	}

	@Override
	public boolean hasDownstreams() {
		RequestIgnoringSubscription<T>[] s = subscribers;
		return s != EMPTY && s != TERMINATED;
	}

	public boolean hasCompleted() {
		return subscribers == TERMINATED && error == null;
	}

	public boolean hasError() {
		return subscribers == TERMINATED && error != null;
	}

	@Override
	public Throwable getError() {
		if (subscribers == TERMINATED) {
			return error;
		}
		return null;
	}

	static final class RequestIgnoringSubscription<T> implements Subscription,
	                                                             Receiver, Producer,
	                                                             Trackable {

		final Subscriber<? super T> actual;

		final RequestIgnoringProcessor<T> parent;

		volatile boolean cancelled;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<RequestIgnoringSubscription>
				REQUESTED =
				AtomicLongFieldUpdater.newUpdater(RequestIgnoringSubscription.class, "requested");

		public RequestIgnoringSubscription(Subscriber<? super T> actual, RequestIgnoringProcessor<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				parent.remove(this);
			}
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public Subscriber<? super T> downstream() {
			return actual;
		}

		@Override
		public long requestedFromDownstream() {
			return 0;
		}

		@Override
		public Processor<T, T> upstream() {
			return parent;
		}

		void onNext(T value) {
			actual.onNext(value);
			if (requested != Long.MAX_VALUE) {
				REQUESTED.decrementAndGet(this);
			}
		}

		void onError(Throwable e) {
			actual.onError(e);
		}

		void onComplete() {
			actual.onComplete();
		}

		@Override
		public boolean isStarted() {
			return parent.isStarted();
		}

		@Override
		public boolean isTerminated() {
			return parent.isTerminated();
		}
	}

	@Override
	public Object upstream() {
		return null;
	}
}
